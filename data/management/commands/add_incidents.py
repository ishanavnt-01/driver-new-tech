from django.core.management.base import BaseCommand

from data.tasks.add_incidents import add_incidents


class Command(BaseCommand):
    help = 'Incident Bulk Upload'

    def handle(self, *args, **options):
        add_incidents()
