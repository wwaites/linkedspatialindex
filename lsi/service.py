from werkzeug.datastructures import Headers
from werkzeug.exceptions import HTTPException, BadRequest, NotFound, NotAcceptable, InternalServerError
from werkzeug.routing import Map, Rule
from werkzeug.wrappers import Request, Response
from autoneg.accept import negotiate
from glob import glob
from os import path
import traceback
from osgeo import ogr
import os
import threading
from rdflib.graph import Graph, ConjunctiveGraph
from rdflib.namespace import RDF
from rdflib.term import URIRef, Literal
from rdflib_rdfjson.rdfjson_parser import RdfJsonParser
try:
    import simplejson as json
except ImportError:
    import json
try:
    from cStringIO import StringiO
except ImportError:
    from StringIO import StringIO
from acora import AcoraBuilder
from decimal import Decimal
from math import cos, radians, degrees
from rtree.index import Property

log = __import__("logging").getLogger("geosvc")

class JsonException(object):
    def __init__(self, exc):
        self.exc = exc
    def __call__(self, environ, start_response):
        status = "%s %s" % (self.exc.code, self.exc.message)
        headers = Headers()
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = len(self.exc.description)
        start_response(status, headers)
        return [self.exc.description]

def tlogwrap(f, *av, **kw):
    def _f():
        try:
            f(*av, **kw)
        except:
            exc = traceback.format_exc()
            log.error(exc)
    return _f

class GeoService(object):
    def __init__(self, config):
        self.url_map = Map([
                Rule('/indexes', endpoint="provision"),
                Rule('/indexes/<index>/reset', endpoint="reset"),
                Rule('/indexes/<index>/search', endpoint="search")
                ])
        self.config = config
        self.datadir = self.config.get("directory", "./")
        self.index_lock = threading.RLock()
        self.indexes = {}
        self.start_indexes()

    def start_indexes(self):
        for index_file in glob(path.join(self.datadir, "*.dat")):
            index = path.basename(index_file)[:-4]
            self.add_index(index)
    
    def add_index(self, index, rebuild=False):
        self.index_lock.acquire()

        index_state = self.indexes.get(index)
        if index_state is not None:
            del self.indexes[index]
            index_state["node"].close()
            t = index_state.get("tail")
            if t is not None and t.isAlive():
                log.info("stopping tail for %s" % index)
                t.join()


        idx_config_file = path.join(self.datadir, index + ".cfg")
        try:
            fp = open(idx_config_file, "r")
            idx_cfg = json.loads(fp.read())
            fp.close()
        except IOError:
            idx_cfg = {}

        rebuild = rebuild or idx_cfg.get("rebuild", False)
        kw = {"rebuild": rebuild}
        idx_cfg["rebuild"] = False
        fp = open(idx_config_file, "w")
        fp.write(json.dumps(idx_cfg))
        fp.close()

        kw["username"] = self.config.get("username")
        kw["password"] = self.config.get("password")
        kw["kernel_host"] = self.config.get("kernel_host")

        if "properties" in idx_cfg:
            p = Property()
            for k,v in idx_cfg["properties"].items():
                setattr(p, k, v)
            kw["properties"] = p

        log.info("opening index on %s" % index)

	# XXXX Implement GeoNode to receive data from somewhere
        node = GeoNode(index, **kw)

        self.indexes[index] = {
            "node": node,
            "config": idx_cfg
            }

        if rebuild or idx_cfg.get("tail", True):
            log.info("starting tail for %s" % index)
            t = threading.Thread(target=tlogwrap(node.tail), name=index)
            t.daemon = True
            self.indexes[index]["tail"] = t
            t.start()

        self.index_lock.release()

        return index

    def reset(self, index):
        self.index_lock.acquire()
        log.info("reset index %s" % index)
        index_state = self.indexes.get(index)
        if index_state is not None:
            del self.indexes[index]
            index_state["node"].close()
        try:
            os.unlink(path.join(self.datadir, index + ".dat"))
        except OSError as e:
            pass
        try:
            os.unlink(path.join(self.datadir, index + ".idx"))
        except OSError as e:
            pass
        try:
            os.unlink(path.join(self.datadir, index + ".kch"))
        except OSError as e:
            pass

        self.add_index(index, rebuild=True)
        self.index_lock.release()

    def dispatch(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except BadRequest, e:
            return JsonException(e)
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        try:
            request = Request(environ)
            response = self.dispatch(request)
            return response(environ, start_response)
        except:
            exc = traceback.format_exc()
            log.error(exc)
            raise InternalServerError()

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

    def on_provision(self, request):
        index = request.values.get("id")
        if index is None:
            msg = { "error": "missing id parameter" }
            raise BadRequest(json.dumps(msg))
        index = self.add_index(index)
        response = {
            "message": "Accepted request to provision a new index",
            }
        return Response(json.dumps(response), mimetype="application/json")

    def on_reset(self, request, index):
        self.index_lock.acquire()
        if index not in self.indexes:
            response = NotFound("index %s" % index)
        else:
            t = threading.Thread(target=tlogwrap(self.reset, index))
            t.start()
            msg = {
                "message": "queued request to reset %s" % index
                }
            response = Response(json.dumps(msg), mimetype="application/json")
        self.index_lock.release()
        return response

    def on_search(self, request, index):
        if index not in self.indexes:
            raise NotFound("index %s" % index)
        node = self.indexes[index]["node"]

        if "predicate" in request.args:
            predicate = request.args["predicate"]
        else:
            predicate = "nearest"
        if predicate not in ["intersects", "contains", "nearest"]:
            msg = { "message": "predicate must be one of intersects, contains, hearest" }
            raise BadRequest(json.dumps(msg))

        operand = None
        if "wkt" in request.args:
            operand = ogr.CreateGeometryFromWkt(request.args["wkt"])
        elif "bbox" in request.args:
            bbox = request.args["bbox"].split(",")
            try:
                miny, minx, maxy, maxx = [Decimal(x.strip()) for x in bbox]
            except:
                msg = { "message": "invalid bounding box" }
                raise BadRequest(json.dumps(msg))
            bbox = "POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))" % (
                minx, miny, minx, maxy, maxx, maxy, maxx, miny, minx, miny
                )
            operand = ogr.CreateGeometryFromWkt(bbox)
        elif "circle" in request.args:
            spec = request.args["circle"].split(",")
            try:
                y,x,r = [Decimal(x.strip()) for x in spec]
            except:
                msg = { "message": "invalid circle specification" }
                raise BadRequest(json.dumps(msg))
            centre = ogr.CreateGeometryFromWkt("POINT(%s %s)" % (x, y))

            ### radius is given in kilometers. the delta is in
            ### degrees. this approximation works only away
            ### from the poles and for small radii. the magic
            ### number is the radius of the earth in meters
            delta = degrees(float(r) / (6371.0 * cos(radians(x))))
            operand = centre.Buffer(delta)

        if operand is None:
            msg = { "message": "missing or invalid spatial argument (bbox or wkt)" }
            raise BadRequest(json.dumps(msg))

        if operand.GetGeometryType() == ogr.wkbPoint:
            operand = operand.Buffer(0.0001)

        if predicate == "intersects":
            results = node.intersection(operand)
        elif predicate == "contains":
            results = node.contains(operand)
        elif predicate == "nearest":
            try:
                limit = int(request.args["limit"])
            except:
                limit = 10
            results = node.nearest(operand, limit)

        try:
            limit = int(request.args["limit"])
        except:
            limit = 10
        if limit > 1000:
            limit = 1000

        try:
            offset = int(request.args["offset"])
        except:
            offset = 0

        if "type" in request.args or "text" in request.args:
            results = parse_graph(results)
        if "type" in request.args:
            results = filter_types(results, *(URIRef(x) for x in request.args.getlist("type")))
        if "text" in request.args:
            results = filter_text(results, request.args["text"])
        if "type" in request.args or "text" in request.args:
            results = trim_graph(results)
        results = filter_offset(results, offset)
        results = filter_limit(results, limit)

        ancfg = (
            ("text", "turtle", ["turtle"]),
            ("text", "javascript", ["rdf-json"]),
            ("text", "n-triples", ["ntriples"]),
            ("text", "n-quads", ["nquads"]),
            ("application", "rdf+xml", ["xml"]),
            ("application", "json", ["rdf-json"]),
            )

        query = request.args.get("query")
        if query is None:
            data = json.dumps(list(results))
            response = Response(data, mimetype="application/json")
        elif query == "closure":
            accept = request.headers.get("Accept", "*/*")
            candidates = list(negotiate(ancfg, accept))
            if len(candidates) == 0:
                raise NotAcceptable()
            mime_type = candidates[0][0]
            format = candidates[0][1][0]
            cg = ConjunctiveGraph()
            for obj in results:
                ### this shouldn't decode / reencode the json here!
                json_description = json.dumps(obj["json_description"])
                g = Graph(identifier=URIRef(obj["graph"]), store=cg.store)
                RdfJsonParser().parse_json(obj["json_description"], g)
            data = cg.serialize(format=format)
            response = Response(data, mimetype=mime_type)
        else:
            raise BadRequest("no idea what kind of query that is")

        return response

def parse_graph(iterable):
    for obj in iterable:
        g = Graph(identifier=obj["graph"])
        RdfJsonParser().parse_json(obj["json_description"], g)
        obj["_graph"] = g
        yield obj

def trim_graph(iterable):
    for obj in iterable:
        del obj["_graph"]
        yield obj

def filter_types(iterable, *uris):
    def m(obj):
        for _,_,t in obj["_graph"].triples((URIRef(obj["uri"]), RDF["type"], None)):
            if t in uris:
                return True
        return False
    for obj in iterable:
        if m(obj):
            yield obj

def filter_text(iterable, text):
    b = AcoraBuilder(text.lower())
    ac = b.build()
    def m(obj):
        for _,_,o in obj["_graph"]:
            if isinstance(o, Literal):
                for _ in ac.findall(o.lower()):
                    return True
        return False
    for obj in iterable:
        if m(obj):
            yield obj

def filter_offset(iterable, n):
    i = 0
    for v in iterable:
        if i < n:
            i += 1
            continue
        yield v

def filter_limit(iterable, n):
    i = 0
    for v in iterable:
        if i >= n:
            break
        yield v
        i += 1

def run_service():        
    from werkzeug.serving import run_simple
    from os import environ
    import daemon
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Linked Spatial Index Service")
    parser.add_argument('--port', metavar='P', type=int,
                        help='port to listen on (4000)',
                        default=4000)
    parser.add_argument('--host', metavar='H', type=str,
                        help='host to listen on (localhost)',
                        default='localhost')
    parser.add_argument('--logfile', metavar='L', type=str,
                        help='file to log to (stderr)')
    parser.add_argument('--daemon', action='store_true',
                        default=False)
    args = parser.parse_args()

    logcfg = {
        "format": '%(asctime)s [%(thread)s] %(message)s',
        "level": logging.DEBUG
        }
    if args.logfile is not None:
        logcfg["filename"] = args.logfile

    config = {
        "directory": "./",
        }

    def svc():
        logging.basicConfig(**logcfg)
        app = GeoService(config)
        run_simple(args.host, args.port, app)

    if args.daemon:
        with daemon.DaemonContext():
            svc()
    else:
        svc()

