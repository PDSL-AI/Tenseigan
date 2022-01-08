#! /usr/bin/env bash
fab reset_website
sleep 3
export C_FORCE_ROOT="true"
nohup python3 manage.py celery worker --loglevel=info --pool=threads --concurrency=1 > celery.log 2>&1 &
sleep 3
nohup python3 manage.py runserver 0.0.0.0:8002 > django.log 2>&1 &
sleep 3
nohup python3 manage.py celerybeat --verbosity=2 --loglevel=info > beat.log 2>&1 &
sleep 3
echo -e "from django.contrib.auth.models import User; \nUser.objects.filter(email='user@email.com').delete(); \nUser.objects.create_superuser('root', 'yiqin0411@qq.com', 'Hust123@$^')" | python manage.py shell
