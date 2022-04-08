bind = "0.0.0.0:6008"
works = 4
backlog = 2048
pidfile = "log/gunicorn.pid"
accesslog = "log/access.log"
errorlog = "log/debug.log"
timeout = 600
debug = False
capture_output = True
