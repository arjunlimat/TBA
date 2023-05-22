from gunicorn.http import wsgi
import os
from TBASourceMatcherV2.settings import APPLICATION_PORT, WORKERS


class Response(wsgi.Response):
    """
    To mask the Server details in return request.
    """
    def default_headers(self, *args, **kwargs):
        headers = super(Response, self).default_headers(*args, **kwargs)
        return [h for h in headers if not h.startswith('Server:')]


wsgi.Response = Response

workers = WORKERS

# reload for any code changes automatically
reload = True

app_port = str(APPLICATION_PORT)
bind = "0.0.0.0:" + app_port

loglevel = "info"

# All http requests served by gunicorn
# if not os.path.isdir(os.path.join(os.getcwd(), "logs")):
#     os.mkdir(os.path.join(os.getcwd(), "logs"))
# logfile = os.path.join(os.getcwd(), 'logs', 'gunicorn.log')

# Run gunicorn as daemon
# Keep this as False if running with Supervisor
#daemon = RUN_AS_DAEMON
daemon = False
