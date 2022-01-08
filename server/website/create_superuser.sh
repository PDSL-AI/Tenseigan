#! /usr/bin/env bash
echo -e "from django.contrib.auth.models import User; \nUser.objects.filter(email='user@email.com').delete(); \nUser.objects.create_superuser('root', 'yiqin0411@qq.com', 'Hust123@$^')" | python manage.py shell
