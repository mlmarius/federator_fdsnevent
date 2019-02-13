import logging
from eposfederator.libs.base.requesthandler import RequestHandler
from eposfederator.libs import downloader, serviceindex
from eposfederator.libs.base.schema import Schema
from eposfederator.libs.downloader import DownloadError
import tornado.iostream
from marshmallow import fields, validate
from webargs.tornadoparser import use_args
from shapely import geometry
import urllib


logger = logging.getLogger(__name__)


class RequestSchema(Schema):

    class Meta():
        dateformat = '%Y-%m-%dT%H:%M:%S.%fZ'
        strict = True

    starttime = fields.DateTime(
        required=False,
        metadata={
            "label": "Minimum time"
        },
        description="Start data selection from this UTC datetime"
    )

    endtime = fields.DateTime(
        required=False,
        metadata={
            "label": "Maximum time"
        },
        description="End data selection at this UTC datetime"
    )

    maxlatitude = fields.Float(
        validate=validate.Range(max=90, min=-90),
        required=False,
        metadata={
            "label": "Maximum latitude"
        },
        description="Maximum latitude"
    )

    minlatitude = fields.Float(
        validate=validate.Range(max=90, min=-90),
        required=False,
        metadata={
            "label": "Minimum latitude"
        },
        description="Minimum latitude"
    )

    maxlongitude = fields.Float(
        validate=validate.Range(max=180, min=-180),
        required=False,
        metadata={
            "label": "Maximum longitude"
        },
        description="Maximum longitude"
    )

    minlongitude = fields.Float(
        validate=validate.Range(max=180, min=-180),
        required=False,
        metadata={
            "label": "Minimum longitude"
        },
        description="Minimum longitude"
    )

    mindepth = fields.Float(
        required=False,
        metadata={
            "label": "Minimum depth"
        },
        description="Minimum depth"
    )

    maxdepth = fields.Float(
        required=False,
        metadata={
            "label": "Maximum depth"
        },
        description="Maximum depth"
    )

    minmagnitude = fields.Float(
        required=False,
        metadata={
            "label": "Minimum magnitude"
        },
        description="Minimum magnitude"
    )

    maxmagnitude = fields.Float(
        required=False,
        metadata={
            "label": "Maximum magnitude"
        },
        description="Maximum magnitude"
    )

    # includeallorigins = fields.Integer(
    #     validate=validate.OneOf([0, 1]),
    #     default=0,
    #     missing=0,
    #     metadata={
    #         "label": "Should all origins be included?"
    #     }
    # )

    includeallorigins = fields.Boolean(
        default=False,
        missing=False,
        metadata={
            "label": "Should all origins be included?"
        }
    )

    # includeallmagnitudes = fields.Integer(
    #     validate=validate.OneOf([0, 1]),
    #     default=0,
    #     missing=0,
    #     metadata={
    #         "label": "Should all magnitudes be included?"
    #     }
    # )

    includeallmagnitudes = fields.Boolean(
        default=False,
        missing=False,
        metadata={
            "label": "Should all magnitudes be included?"
        }
    )

    # includearrivals = fields.Integer(
    #     validate=validate.OneOf([0, 1]),
    #     default=0,
    #     missing=0,
    #     metadata={
    #         "label": "Should arrivals be included?"
    #     }
    # )

    includearrivals = fields.Boolean(
        default=False,
        missing=False,
        metadata={
            "label": "Should arrivals be included?"
        }
    )

    eventid = fields.String(
        metadata={
            "label": "Event ID"
        },
        validate=validate.Length(min=1, max=300),
        description="Event ID"
    )

    limit = fields.Integer(
        metadata={
            "label": "How many results to return"
        },
        description="How many results to return"
    )

    offset = fields.Integer(
        metadata={
            "label": "Offset from the begining of the result set"
        },
        description="Offset from the begining of the result set"
    )

    contributor = fields.String(
        metadata={
            "label": "Contributor"
        },
        validate=validate.Length(min=1, max=300),
        description="Contributor"
    )

    catalog = fields.String(
        metadata={
            "label": "Catalog"
        },
        validate=validate.Length(min=1, max=300),
        description="Catalog"
    )

    updatedafter = fields.DateTime(
        required=False,
        metadata={
            "label": "Updated after"
        },
        description="Updated after"
    )

    type = fields.String(
        validate=validate.OneOf(["xml", "txt"]),
        default="xml",
        missing="xml",
        metadata={
            "label": "Format of returned data"
        }
    )

    nodate = fields.String(
        metadata={
            "label": "No data"
        },
        validate=validate.Length(min=1, max=300),
        description="No data"
    )

    latitude = fields.Float(
        validate=validate.Range(max=90, min=-90),
        required=False,
        metadata={
            "label": "Latitude"
        },
        description="Latitude"
    )

    longitude = fields.Float(
        validate=validate.Range(max=180, min=-180),
        required=False,
        metadata={
            "label": "Longitude"
        },
        description="Longitude"
    )

    minradius = fields.Float(
        required=False,
        metadata={
            "label": "Minimum radius"
        },
        description="Minimum radius"
    )

    maxradius = fields.Float(
        required=False,
        metadata={
            "label": "Maximum radius"
        },
        description="Maximum radius"
    )


async def extractor(resp, **kwargs):
    chunk_size = kwargs.get('chunk_size', 2000)
    inside_object = False
    obj_start_pattern = '<event '
    obj_end_pattern = '</event>'
    buff_trim = -1 * len(obj_start_pattern + obj_end_pattern)

    buff = ''
    while True:
        crt = await resp.content.read(chunk_size)
        if crt:
            buff += crt.decode()
        else:
            break

        if buff is '':
            break

        # process the current buffer and try yielding found objects
        while True:
            if not inside_object:
                try:
                    obj_start_idx = buff.index(obj_start_pattern)
                    buff = buff[obj_start_idx:]
                    inside_object = True
                except ValueError:
                    buff = buff[buff_trim:]
                    break
            else:
                try:
                    obj_end_idx = buff.index(obj_end_pattern) + len(obj_end_pattern)
                    obj = buff[:obj_end_idx]
                    buff = buff[obj_end_idx:]
                    inside_object = False
                    yield obj
                except ValueError:
                    break


def response_validator(resp, **kwargs):
    content_type = kwargs.get('content-type', 'application/xml')
    status = kwargs.get('status', 200)
    if resp.headers.get('Content-Type') != content_type or resp.status != status:
        raise DownloadError(
            "Invalid xml response from NFO",
            url=resp.url
        )


class Handler(RequestHandler):

    ID = 'event'
    DESCRIPTION = 'Federated FDSN Events endpoint'
    RESPONSE_TYPE = 'application/xml'
    REQUEST_SCHEMA = RequestSchema
    ROUTE = "event"

    @use_args(RequestSchema)
    async def get(self, reqargs):

        try:
            # attempt to define the geographic area for this query
            bounds = geometry.Polygon([
                (reqargs['minlongitude'], reqargs['minlatitude']), (reqargs['maxlongitude'], reqargs['minlatitude']),
                (reqargs['maxlongitude'], reqargs['maxlatitude']), (reqargs['minlongitude'], reqargs['maxlatitude'])
            ])
        except Exception as e:
            logger.error(e, exc_info=True)
            bounds = None

        for key in ['starttime', 'endtime']:
            try:
                reqargs[key] = reqargs[key].strftime('%Y-%m-%dT%H:%M:%S.000Z')
            except Exception:
                pass

        self.set_header('Content-Type', self.RESPONSE_TYPE)
        args = urllib.parse.urlencode(reqargs, safe=':')

        def ffunc(wspointer):
            return wspointer.handler == self.__class__

        urls = serviceindex.get(geometry=bounds, filter_func=ffunc)
        urls = [f"{url.url}?{args}" for url in urls]

        self.write("<?xml version='1.0' encoding='utf8'?><q:quakeml xmlns='http://quakeml.org/xmlns/bed/1.2' xmlns:q='http://quakeml.org/xmlns/quakeml/1.2'><eventParameters publicID='federated_query'>")

        dlmgr = None
        try:
            # ask a dload manager to perform the downloads for us
            # and store the download errors
            dlmgr = downloader.DownloadManager(*urls)
            async for chunk in dlmgr.fetch(extractor=extractor, response_validator=response_validator, timeout_total=60):
                self.write(chunk)
                await self.flush()
        except tornado.iostream.StreamClosedError:
            logger.warning("Client left. Aborting download from upstream.")
            return

        if dlmgr is not None and len(dlmgr.errors) > 0:
            for error in dlmgr.errors:
                logger.warning(str(error))

        self.write('</eventParameters></q:quakeml>')

        await self.flush()
