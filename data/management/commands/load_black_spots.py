import argparse

from django.core.management.base import BaseCommand

from data.tasks.load_black_spots import load_black_spots



class Command(BaseCommand):
    help = 'Incident Bulk Upload'

    def handle(self, *args, **options):
        load_black_spots()
