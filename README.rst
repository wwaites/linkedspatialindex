Linked Spatial Index
====================

Spatial Indexing for Linked Data Systems.

Public API
----------

To create a new index, a request should be made to:

    http://geo.example.org/indexes

As in::

    curl --digest -u "user:pass" -d "bucket_id=BUCKET_ID" \
        http://geo.example.org/indexes

which should give a response like::

    {
      "message":"Accepted request to provision a new index",
    }

The search endpoint,

    http://geo.example.org/indexes/INDEX_ID/search

will take several parameters,

=============  ===========
Parameter      Description
=============  ===========
**predicate**  one of "intersects", "contains", "nearest" 
**wkt**        well-known-text for the argument to the predicate
**bbox**       bounding box in the form "minlat, minlong, maxlat, maxlong"
**circle**     point and radius in the form "lat,long,radius" (radius
               in km)
**limit**      number of expected results for the query
**offset**     skip the first n results of the query
**query**      Can contain a SPARQL query to be run on the result set,
               or the value "closure" to return a complete description
	       of the matching entities, or if not specified a simple
	       JSON encoded list of matching entities is returned
**type**       Filter results by the provided RDF type
**text**       Filter results by literals containing the given text
=============  ===========

*Note: parameters are tentative pending implementation*

As noted above, the result of a query can be either a list of URIs for
entities that have a spatial component that matches, or an RDF graph
(subject to the usual content autonegotiation) containing their
complete descriptions, or a SPARQL result set further narrowing the
results according to a particular query.

The reset endpoint,

    http://geo.example.org/indexes/INDEX_ID/reset

does what it says on the tin, and purges the endpoint and causes a
rebuild of the index. Obviously this can be computationally
expensive. It does this in parallel with the running index, however,
and they are only swapped when the rebuild is complete.

Theory of Operation
-------------------

Spatial indexing is all about the sweep and prune pattern.

Things to be indexed, which may be of a complicated shape, are put
into an R-tree according to their bounding box. The same thing happens
to query parameters that contain complicated shapes. By complicated, I
mean anything from the perimiter of a city to polygons with holes, for
example countries like South Africa that encircle entire other
countries.

So these complicated shapes are reduced to the smallest rectangle that
encloses them, and then a rough result set is produced by doing fairly
simple operations on these rectangles. The rough result set is then
narrowed down by doing the more computationally intensive calculation
over whether the complicated shapes do indeed match.

A second level of pruning is then supported by this
implementation. Because the result of the spatial query are
descriptions in RDF, we can make a temporary, in-memory RDF graph
for this. On this temporary graph we can then run a SPARQL query,
which may be supplied, to further reduce the result set, or to project
the result set into a different form as required by the client
application.

Implementation Details
----------------------

An index is provisioned, it needs to be fed with data from somewhere.
It looks at the incoming data in searching for things of the following
forms::


    ?foo wgs84:lat ?lat;
         wgs84:long ?long.

    ?foo dct:spatial [
         geosparql:asWKT ?wkt
    ].

    ?foo dct:spatial [
         geosparql:asGML ?gml
    ].

    ?foo georss:pont ?point.

And in each of these cases, the resource that will be returned or
described or otherwise stored in the spatial index is ?foo.

The spatial index is done with libspatialindex via the python R-tree
bindings. What is stored is in fact a JSON object of the form::

    { "uri": resource,
      "graph": graph,
      "geom": geometry_in_wkt,
      "json_description": description_encoded_as_rdf_json }

The *geom* field is used to do the first pruning pass.

The *text* field is the serialised description which is either
returned or used for further pruning with SPARQL.

This JSON blob is stored in the index with the identifier being the
FNV1a hash of the URI - in order to support deletion or replacement
from the index.

Installation
------------

Prerequisites:

  * GDAL with python bindings and GEOS support enabled
  * libspatialindex
  * kyoto cabinet and python bindings
  * pip install -e git+git@github.com:RDFLib/rdflib-rdfjson#egg=rdflib_rdfjson

Typically these will be installed using python's
virtualenv(1). Standard practice is to make sure the service runs as a
dedicated user. A good choice is to make one called `geo`. Typically
the virtualenv will be initialised with a command like::

    virtualenv ~geo

And then this line will be added to the user's shell startup files,
along with any relevant environment variables::

    . ~/bin/activate

The effect of this is that when the geo user runs python or any
related commands, the version in `~geo/bin` will be used and the
environment will be correct.

This package is installed in the usual python way, simply by doing::

    python setup.py install

Operation
=========

The main command to have the indexer run is `lsi`. This must be
run from the directory where the data files or on-disk indexes are to
live. This might be a directory like `~geo/data`. The output from the
command may be redirected to a log file or simply left to go to stdout
and run under a screen session.

The default is for the command to listen on `localhost:4000`. This can
be changed with command line switches. Usually a reverse proxy such as
nginx will listen on port 80 and redirect traffic to this service.

Bugs
====

Sometimes on the receipt of a `reset` command, the index is not
correctly purged. In this case, a workaround is to (1) stop the
service (2) remove the relevant data files (3) restart the service
and (4) reprovision the index.
