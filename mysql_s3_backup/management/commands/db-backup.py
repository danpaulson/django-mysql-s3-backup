import boto3
import datetime
import os

from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--daily',
            action='store_true',
            help='Modify the object_name to include the day of the week.',
        )

    def handle(self, *args, **options):
        kwargs = {
            'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
            'region_name': settings.AWS_S3_REGION_NAME
        }

        bucket_name = settings.AWS_BACKUP_BUCKET
        if options['daily']:
            day_of_week = datetime.datetime.now().strftime('%A').lower()
            object_name = f'{bucket_name}.{day_of_week}.sql'
        else:
            object_name = f'{bucket_name}.sql'

        db_file_path = '/tmp/db.sql'

        s3 = boto3.client('s3', **kwargs)

        print('# Create DB Dump')
        os.system('mysqldump --single-transaction --quick --complete-insert --lock-tables=false -h{0} -u{1} -p{2} {3} > {4}'.format(
            settings.DATABASES['default']['HOST'],
            settings.DATABASES['default']['USER'],
            settings.DATABASES['default']['PASSWORD'],
            settings.DATABASES['default']['NAME'],
            db_file_path
        ))
        s3.upload_file(db_file_path, bucket_name, object_name)
        os.remove(db_file_path)
