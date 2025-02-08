from django.core.management.base import BaseCommand

from data.tasks.create_dataset import create_dataset


class Command(BaseCommand):
    help = 'Create Initial Dataset'

    def handle(self, *args, **options):
        create_dataset()
