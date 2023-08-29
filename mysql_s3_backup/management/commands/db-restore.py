import boto3
import os

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force run the command even outside of dev.',
        )

    def handle(self, *args, **options):
        if not settings.DEBUG and not options['force']:
            raise CommandError('Cannot run outside of dev unless --force is provided.')

        bucket_name = settings.AWS_BACKUP_BUCKET
        object_name = '{}.sql'.format(settings.AWS_BACKUP_BUCKET)
        db_file_path = '/tmp/db.sql'
        kwargs = {
            'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
            'region_name': settings.AWS_S3_REGION_NAME
        }

        s3 = boto3.client('s3', **kwargs)
        last_updated = s3.get_object(Bucket=bucket_name, Key=object_name)['LastModified']

        print('Restoring to host: {}'.format(settings.DATABASES['default']['HOST']))
        answer = input('Restore Database from S3, last updated {0}? THIS WILL DESTROY YOUR LOCAL DB! [Y/N] '.format(last_updated))

        if answer == 'Y':
            print('# Downloading DB Dump')
            s3.download_file(bucket_name, object_name, db_file_path)
            print('# Importing DB Dump')
            os.system('mysql -h{0} -u{1} -p{2} --database={3} < {4}'.format(
                settings.DATABASES['default']['HOST'],
                settings.DATABASES['default']['USER'],
                settings.DATABASES['default']['PASSWORD'],
                settings.DATABASES['default']['NAME'],
                db_file_path
            ))
            print('# Removing DB File')
            os.remove(db_file_path)

        else:
            raise CommandError('Aborted.')
