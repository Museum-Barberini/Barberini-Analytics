import os
import django
from django.conf import settings
from django.template.loader import render_to_string

settings.configure(
    TEMPLATES=[{
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.dirname(os.path.realpath(__file__))],  # script dir
    }]
)
django.setup()

message = render_to_string(
    'data/strings/long_stage_failure_email.html',
    {'CI_PIPELINE_URL': 1337}
)
print(message)
