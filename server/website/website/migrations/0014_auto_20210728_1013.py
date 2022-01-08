# -*- coding: utf-8 -*-
# Generated by Django 1.11.27 on 2021-07-28 14:13
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('website', '0013_backupdata_other'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dbmscatalog',
            name='type',
            field=models.IntegerField(choices=[(1, 'MySQL'), (2, 'Postgres'), (3, 'Db2'), (4, 'Oracle'), (6, 'SQLite'), (7, 'HStore'), (8, 'Vector'), (5, 'SQL Server'), (9, 'MyRocks'), (10, 'TiDB')]),
        ),
    ]
