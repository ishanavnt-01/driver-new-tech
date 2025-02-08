# -*- coding: utf-8 -*-
"""
    *************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    *************************************************************************************************

    This class deals with the authentication. It generates the signatures for app and user and makes
    them available to the APIRequest object, for sending with all API requests. 
"""
from __future__ import absolute_import

from py3rijndael import RijndaelCbc, ZeroPadding
import hmac
from hashlib import md5, sha256
from base64 import b64encode
from json import dumps


class BaseAuthentication(object):

    def __init__(self):
        """
            Headers for authentication such as the auth_id and API key.
            :rtype dict - name value pairs. e.g. dict(auth_id=5)
        """
        self.auth_headers = dict()

    @staticmethod
    def enctrypt(message, key):
        """
        ORIGINAL PIP METHOD - DOES NOT WORK - see encrypt method below
            Encrypt a String
        
        :param message: the message to encrypt
        :param key: the key to encrypt and then decrypt the message.
        :return: the encrypted form of the string
        """
        KEY_SIZE = 16
        BLOCK_SIZE = 32
        padded_key = key.ljust(KEY_SIZE, '\0')
        padded_text = message + (BLOCK_SIZE - len(message) % BLOCK_SIZE) * '\0'

        md5_key = md5(padded_key).hexdigest()
        r = rijndael.rijndael(md5_key, BLOCK_SIZE)

        ciphertext = ''
        for start in range(0, len(padded_text), BLOCK_SIZE):
            ciphertext += r.encrypt(padded_text[start:start + BLOCK_SIZE])

        return b64encode(ciphertext)

    @staticmethod
    def encrypt(message, key):
        """
            Encrypt a String
            This is the working version compatible with the vida api

        :param message: the message to encrypt
        :param key: the key to encrypt and then decrypt the message.
        :return: the encrypted form of the string
        """
        # Hash the key, then hash the hash to get the iv parameter for the encryption
        md5_key = md5(bytes(key, 'utf-8'))
        hex_md5_key = md5_key.hexdigest()
        iv = md5(bytes(hex_md5_key, 'utf-8')).hexdigest()

        # Pad the message (this is copied from the original PIP method
        BLOCK_SIZE = 32
        padded_text = message + (BLOCK_SIZE - len(message) % BLOCK_SIZE) * '\0'

        # Instantiate our encryption library (had to change as original did not work with Python 3
        rijndael_cbc = RijndaelCbc(
            key=hex_md5_key,
            iv=iv,
            padding=ZeroPadding(32),
            block_size=BLOCK_SIZE)

        # Encrypt then base 64 encode
        cipher = rijndael_cbc.encrypt(bytes(padded_text, 'utf-8'))

        b64_cipher = b64encode(cipher)

        # Convert to string for return
        return b64_cipher.decode('utf-8')

    @staticmethod
    def generate_signature(parameters, secret_key):

        """
            Takes the request parameters and the secret key and generates the request signature using
            the hmac method and the sha256 algorithm.
            
        :param parameters: dict of parameters
        :param secret_key: string
        :return: string
        """
        if isinstance(secret_key, str):
            secret_key = bytes(secret_key, 'utf-8')
        lower_parameters = {k.lower(): v for k, v in parameters.items()}
        json_string = dumps(lower_parameters, separators=(',', ':'), sort_keys=True).replace('/', r'\/')
        signature = hmac.new(secret_key, json_string.encode('utf-8'), sha256).hexdigest()

        return signature

    def get_signatures(self, data):
        raise NotImplementedError

    def get_auth_headers(self):
        raise NotImplementedError


