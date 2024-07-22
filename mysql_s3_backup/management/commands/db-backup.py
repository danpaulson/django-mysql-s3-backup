import boto3
import datetime
import logging
import os

from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            help='Specify a different database name. Defaults to the one in settings.',
            default=None  # Default value if not provided.
        )

    def handle(self, *args, **options):
        s3_kwargs = {
            'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
            'region_name': settings.AWS_S3_REGION_NAME
        }

        bucket_name = settings.AWS_BACKUP_BUCKET
        database_name = options['name'] if options['name'] else settings.DATABASES['default']['NAME']
        directory = f'{settings.AWS_BACKUP_DIRECTORY}/' if settings.AWS_BACKUP_DIRECTORY else ''
        prefix = f'{directory}db-backup-{database_name}.'
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        object_name = f'{prefix}{date_str}.sql'

        db_file_path = '/tmp/db.sql'

        s3 = boto3.client('s3', **s3_kwargs)

        logging.info('# Create DB Dump')
        os.system('mysqldump --single-transaction --quick --complete-insert --lock-tables=false -h{0} -u{1} -p{2} {3} > {4}'.format(
            settings.DATABASES['default']['HOST'],
            settings.DATABASES['default']['USER'],
            settings.DATABASES['default']['PASSWORD'],
            database_name,
            db_file_path
        ))
        s3.upload_file(db_file_path, bucket_name, object_name)
        os.remove(db_file_path)

        # Removing backups older than 7 days
        date_threshold = datetime.datetime.now() - datetime.timedelta(days=7)
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}")['Contents']

        for obj in objects:
            print(obj)
            # Extract the date from the object key
            date_part = obj['Key'].split('.')[1]
            backup_date = datetime.datetime.strptime(date_part, '%Y-%m-%d')

            if backup_date < date_threshold:
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                logging.info(f'Removed old backup: {obj["Key"]}')
