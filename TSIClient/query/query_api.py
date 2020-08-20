from ..authorization.authorization_api import AuthorizationApi


class QueryApi():
    def __init__(self, authorization_api: AuthorizationApi):
        self.authorization_api = authorization_api
