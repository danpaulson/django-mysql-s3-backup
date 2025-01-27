import boto3
import datetime
import os

from prompt_toolkit.shortcuts import radiolist_dialog

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force run the command even outside of dev.',
        )
        parser.add_argument(
            '--yes',
            action='store_true',
            help='Skip the confirmation prompt.',
        )
        parser.add_argument(
            '-name',
            type=str,
            help='Specify a different database name. Defaults to the one in settings.',
            default=None  # Default value if not provided.
        )
        parser.add_argument(
            '-local_db_name',
            type=str,
            help='Specify a different database name locally.',
            default=None  # Default value if not provided.
        )
        parser.add_argument(
            '--choose',
            action='store_true',
            help='Choose the backup from a list.',
        )
        parser.add_argument(
            '--no-delete',
            action='store_true',
            help='Do not delete the file after the import and use timestamp as file name.'
        )

    def handle(self, *args, **options):
        def size_format(num, suffix='B'):
            for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
                if abs(num) < 1024.0:
                    return "%3.1f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Y', suffix)

        if not settings.DEBUG and not options['force']:
            raise CommandError('Cannot run outside of dev unless --force is provided.')

        s3_kwargs = {
            'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
            'region_name': settings.AWS_S3_REGION_NAME
        }
        s3 = boto3.client('s3', **s3_kwargs)

        db_file_name = 'db.sql'
        db_file_path = os.path.join(getattr(settings, 'BACKUP_DB_FILE_PATH', '/tmp/'), db_file_name)

        bucket_name = settings.AWS_BACKUP_BUCKET
        database_name = options['name'] if options['name'] else settings.DATABASES['default']['NAME']
        directory = f'{settings.AWS_BACKUP_DIRECTORY}/' if settings.AWS_BACKUP_DIRECTORY else ''
        prefix = f'{directory}db-backup-{database_name}.'
        # Get all objects in the bucket with the given prefix
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Check if we received any objects
        if 'Contents' not in objects:
            raise CommandError(f'No backups found for prefix: {prefix}')

        # Sort objects by their last modified date, most recent first
        sorted_objects = sorted(objects['Contents'], key=lambda obj: obj['LastModified'], reverse=True)

        # Get the most recent object's key
        if options['choose']:
            # Preparing the choices for the radiolist
            choices = [
                (
                    obj['Key'], 
                    f"{obj['Key']} ({obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')} - {size_format(obj['Size'])})"
                ) 
                for obj in sorted_objects
            ]

            object_name = radiolist_dialog(
                title="Choose a backup",
                text="Use the arrow keys to navigate and select a backup.",
                values=choices,
            ).run()

            if not object_name:
                raise CommandError("No backup selected.")
        else:
            # Your current logic of selecting the most recent backup
            object_name = sorted_objects[0]['Key']

        # Extract the date from the object_name
        # Assuming format is 'db-backup-{database_name}.{date}.sql'
        object_metadata = s3.head_object(Bucket=bucket_name, Key=object_name)
        backup_datetime = object_metadata['LastModified']

        current_datetime = datetime.datetime.now(datetime.timezone.utc)  # To match timezone-aware datetime from S3
        time_difference = current_datetime - backup_datetime
        days = time_difference.days

        if days == 0:
            hours = time_difference.seconds // 3600
            if hours == 0:
                minutes = time_difference.seconds // 60
                time_str = f"{minutes}m old"
            else:
                time_str = f"{hours}h old"
        elif days < 30:
            time_str = f"{days}h old"
        elif days < 365:
            months = days // 30  # Simplified, not accounting for variable month lengths
            time_str = f"{months}m old"
        else:
            years = days // 365  # Simplified, not accounting for leap years
            time_str = f"{years}y old"

        download_new_file = True
        if options['no_delete']:
            if os.path.exists(db_file_path):
                overwrite = input(f"File {db_file_path} already exists. Download new? [Y/N]: ")
                if overwrite.upper() != 'Y':
                    download_new_file = False

        # DB info
        db_host = settings.DATABASES['default']['HOST']

        # Local DB Name override
        if options['local_db_name']:
            database_name = options['local_db_name']

        print('')
        print(f'Target Host: {db_host}')
        print(f'Database:    {database_name}')
        print(f'Backup:      {object_name}')
        print(f'             {time_str}')
        print(f'             {size_format(object_metadata["ContentLength"])}')
        print('')
        
        if not options['yes']:
            answer = input('Restore Database from S3? [Y/N]: ')
            if answer != 'Y':
                raise CommandError('Aborted.')

        if download_new_file:
            print('# Downloading DB Dump')
            s3.download_file(bucket_name, object_name, db_file_path)

        print('# Importing DB Dump')
        os.system('mysql -h{0} -u{1} -p{2} --database={3} < {4}'.format(
            db_host,
            settings.DATABASES['default']['USER'],
            settings.DATABASES['default']['PASSWORD'],
            database_name,
            db_file_path
        ))
        if not options['no_delete']:
            print('# Removing DB File')
            os.remove(db_file_path)
