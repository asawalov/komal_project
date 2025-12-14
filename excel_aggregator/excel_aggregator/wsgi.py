"""
WSGI config for excel_aggregator project.
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'excel_aggregator.settings')

application = get_wsgi_application()

