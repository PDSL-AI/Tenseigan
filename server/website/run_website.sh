#! /usr/bin/env bash
export C_FORCE_ROOT="true"
nohup python3 manage.py celery worker --loglevel=info --pool=threads --concurrency=1 > celery.log 2>&1 &
nohup python3 manage.py runserver 0.0.0.0:8002 > django.log 2>&1 &
nohup python3 manage.py celerybeat --verbosity=2 --loglevel=info > beat.log 2>&1 &
