"""
>>> from rdflib.graph import ConjunctiveGraph
>>> from StringIO import StringIO
>>>
>>> ex1 = '''
... @prefix wgs84: <http://www.w3.org/2003/01/geo/wgs84_pos#>.
...
... <http://example.org/foo> a wgs84:SpatialThing;
...    wgs84:lat "10.0";
...    wgs84:long "10.0".
... '''
...
>>> g = Graph(identifier=URIRef("http://example.org/ex1"))
>>> g.parse(StringIO(ex1), format="n3")
<Graph identifier=http://example.org/ex1 (<class 'rdflib.graph.Graph'>)>
>>> text = ConjunctiveGraph(g.store).serialize(format="nquads")
>>> tree = LinkedRtree()
>>> tree.addNQ(StringIO(text))
>>> centre = ogr.CreateGeometryFromWkt("POINT(0.0 0.0)")
>>> len(list(tree.nearest(centre, 10))) == 1
True
>>>

>>> ex2 = '''
... @prefix ogs: <http://www.opengis.net/ont/OGC-GeoSPARQL/1.0/>.
... @prefix ogt: <http://www.opengis.net/def/dataType/OGC-SF/1.0/>.
... @prefix dct: <http://purl.org/dc/terms/>.
...
... <http://example.org/bar> dct:spatial [
...    a ogs:Geometry;
...    ogs:asWKT ""\"<http://www.opengis.net/def/crs/OGC/1.3/CRS84>
...               POLYGON((-83.6 34.1, -83.2 34.1, -83.2 34.5,
...                        -83.6 34.5, -83.6 34.1))""\"^^ogt:WKTLiteral
...    ].
... '''
>>> g = Graph(identifier=URIRef("http://example.org/ex2"))
>>> g.parse(StringIO(ex2), format="n3")
<Graph identifier=http://example.org/ex2 (<class 'rdflib.graph.Graph'>)>
>>> text = ConjunctiveGraph(g.store).serialize(format="nquads")
>>> def describe(obj):
...     d = Graph()
...     for statement in g.triples((None, None, obj)):
...         d.add(statement)
...     return d
...
>>> tree = LinkedRtree(describe=describe)
>>> tree.addNQ(StringIO(text))
>>> overlap = "POLYGON((-83.8 34.1, -83.4 34.1, -83.4 34.4, -83.8 34.4, -83.8 34.1))"
>>> overlap = ogr.CreateGeometryFromWkt(overlap)
>>> len(list(tree.intersection(overlap))) == 1
True
>>> overlap2 = "POINT(-83.4 34.3)"
>>> overlap2 = ogr.CreateGeometryFromWkt(overlap2)
>>> len(list(tree.intersection(overlap2))) == 1
True
>>>
>>> noverlap = "POLYGON((0.0 0.0, 10.0 0.0, 10.0 10.0, 0.0 10.0, 0.0 0.0))"
>>> noverlap = ogr.CreateGeometryFromWkt(noverlap)
>>> len(list(tree.intersection(noverlap))) == 0
True

"""

from rdflib.namespace import Namespace
from rdflib.graph import Graph
from rdflib.parser import create_input_source
from rdflib.plugins.parsers.nquads import NQuadsParser
from rdflib.term import BNode, URIRef
import geojson
from shapely.geometry import asShape
from osgeo import gdal
from osgeo import ogr
import rtree
import kyotocabinet as kc
try:
    import simplejson as json
except ImportError:
    import json

WGS84 = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
GEOSPARQL = Namespace("http://www.opengis.net/ont/OGC-GeoSPARQL/1.0/")
GEORSS = Namespace("http://www.georss.org/georss/")
OSG = Namespace("http://data.ordnancesurvey.co.uk/ontology/geometry/")

LAT = unicode(WGS84["lat"])
LONG = unicode(WGS84["long"])
ASWKT = unicode(GEOSPARQL["asWKT"])
POINT = unicode(GEORSS["point"])
ASGEOJSON = unicode(OSG["asGeoJSON"])
ASGML = unicode(OSG["asGML"])

import re
_tok = re.compile(r'(\s+)')
def tok(s):
    return [x for x in _tok.split(s) if not _tok.match(x)]

class SpatialStore(object):
    context_aware = True
    def __init__(self, tree):
        self.tree = tree
        self.state = {}

    def add(self, (s,p,o), g):
        if self.state.get("uri") != unicode(s) or self.state.get("graph") != unicode(g):
            self.finalise()
            self.state = {
                "uri": unicode(s),
                "graph": unicode(g),
                "description": Graph()
                }

        self.state["description"].add((s,p,o))

        if p in (WGS84["lat"], WGS84["long"], GEORSS["point"], GEOSPARQL["asWKT"], OSG["asGeoJSON"], OSG["asGML"]):
            self.state[unicode(p)] = unicode(o)

    def finalise(self):
        if ASWKT in self.state:
            crs_wkt = self.state[ASWKT]
            crs, wkt = crs_wkt.strip().split(" ", 1)
            self.state["geom"] = wkt.strip().replace("\n", " ").replace("  ", " ")
            ## so because of the indirection here with GeoSPARQL, we have to
            ## describe the resource in some other way... so how do we do that?
            ## we use the passed in describe function which may well go and hit
            ## the remote service or something...
            if hasattr(self.tree, "describe"):
                ### xxx kludgy
                if ":" in self.state["uri"]:
                    duri = URIRef(self.state["uri"])
                else:
                    duri = BNode(self.state["uri"])
                self.state["description"] += self.tree.describe(duri)
                for s,_,_ in self.state["description"].triples((None, None, duri)):
                    self.state["uri"] = unicode(s)

#        elif ASGML in self.state:
#            geom = ogr.CreateGeometryFromGML(self.state[ASGML])
#            print geom

        elif ASGEOJSON in self.state:
            data = json.loads(self.state[ASGEOJSON])
            if 'geometry' in data:
                data = data['geometry']
            feat = geojson.GeoJSON(**data)
            shape = asShape(feat)
            self.state["geom"] = shape.wkt
            ### as with WKT
            if hasattr(self.tree, "describe"):
                ### xxx kludgy
                if ":" in self.state["uri"]:
                    duri = URIRef(self.state["uri"])
                else:
                    duri = BNode(self.state["uri"])
                self.state["description"] += self.tree.describe(duri)
                for s,_,_ in self.state["description"].triples((None, None, duri)):
                    self.state["uri"] = unicode(s)

        elif LAT in self.state and LONG in self.state:
            wkt = u"POINT(%s %s)" % (self.state[LONG], self.state[LAT])
            self.state["geom"] = wkt
        elif POINT in self.state:
            pt = tok(self.state[POINT])
            wkt = u"POINT(%s %s)" % (pt[1], pt[0])
            self.state["geom"] = wkt

        ## this is getting a little kludgy
        for k in (ASWKT, ASGEOJSON, LAT, LONG, POINT):
            if k in self.state:
                del self.state[k]

        if "geom" in self.state:
            json_text = self.state["description"].serialize(format="rdf-json")
            self.state["json_description"] = json.loads(json_text)
            del self.state["description"]
            geom = ogr.CreateGeometryFromWkt(self.state["geom"])
            if geom is not None:
                ident = hash(self.state["uri"] + self.state["graph"])
                self.tree.delete(ident, [-180, 180, -90, 90])
                self.tree.add(ident, geom.GetEnvelope())
                self.tree.kch.set(ident, json.dumps(self.state))

class QuadSink(object):
    def __init__(self, tree):
        self.store = SpatialStore(tree)

class LinkedRtree(rtree.Rtree):
    """
    This is the glue between RDF and Rtree. The method addNQ adds
    quads from a parseable stream. The nearest and intersection
    methods do as with Rtree but they are slightly smarter and can use
    the geometry types from OGR for sweep-and-prune.

    It is not thread safe! Protect with a mutex!

    The describe method that can be optionally passed to the
    constructor is important for GeoSPARQL-esque things. This is
    because of the level of indirection:

        <foo> a Thing;
            dct:spatial [
               a ogs:Geometry;
               ogs:asWKT "..."
            ].

    without this describe method, what gets put in the index is just
    the blank node. Which is not very much good for anything, now is
    it?
    """
    dumps = staticmethod(json.dumps)
    loads = staticmethod(json.loads)

    def __init__(self, filename=None, describe=None, **kw):
        if describe is not None:
            self.describe = describe
        self.kch = kc.DB()
        kwc = kw.copy()
        kwc["interleaved"] = False
        if filename is not None:
            av = [filename]
            self.kch.open(filename + ".kch", kc.DB.OWRITER | kc.DB.OCREATE | kc.DB.ONOREPAIR)
        else:
            av = []
            self.kch.open("*", kc.DB.OWRITER)
        super(LinkedRtree, self).__init__(*av, **kwc)

    def close(self):
        super(LinkedRtree, self).close()
        self.kch.close()

    def addNQ(self, quadio):
        sink = QuadSink(self)
        nqp = NQuadsParser(sink)
        nqp.parse(create_input_source(quadio), sink)
        sink.store.finalise()

    def nearest(self, geom, limit=10):
        if geom.GetGeometryType() == ogr.wkbPoint:
            centroid = geom
        else:
            centroid = geom.Centroid()
        geom = (centroid.GetX(), centroid.GetY())
        for obj in super(LinkedRtree, self).nearest(geom, limit):
            data = self.kch.get(obj)
            if data is not None:
                yield json.loads(data)

    def intersection(self, geom):
        ### sweep and prune
        bbox = geom.GetEnvelope()
        for obj in super(LinkedRtree, self).intersection(bbox):
            data = self.kch.get(obj)
            if data is None:
                continue
            robj = json.loads(data)
            rgeom = ogr.CreateGeometryFromWkt(robj["geom"])
            if geom.Intersect(rgeom):
                yield robj

    def contains(self, geom):
        ### sweep and prune
        bbox = geom.GetEnvelope()
        for obj in super(LinkedRtree, self).intersection(bbox):
            data = self.kch.get(obj)
            if data is None:
                continue
            robj = json.loads(data)
            rgeom = ogr.CreateGeometryFromWkt(robj["geom"])
            if geom.Contains(rgeom):
                yield robj

if __name__ == '__main__':
    import doctest
    doctest.testmod()
