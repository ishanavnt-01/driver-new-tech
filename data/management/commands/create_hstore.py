from django.core.management.base import BaseCommand

from data.tasks.test_extension import create_extension


class Command(BaseCommand):
    help = 'Create Initial Dataset'

    def handle(self, *args, **options):
        create_extension()