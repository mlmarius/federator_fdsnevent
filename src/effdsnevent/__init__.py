from eposfederator.libs import appbuilder

NAME = 'FDS Event'
ID = 'effdsnevent'
DESCRIPTION = "Federates FDSN Event webservices"
BASE_ROUTE = '/fdsn'

# collect all handlers from this plugin's 'handlers' module
HANDLERS = list(appbuilder.collect_handlers(f"{__name__}.handlers"))
