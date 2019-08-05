import json

class Response:
    @staticmethod
    def json(errorcode=0, errormsg='ok', **kwargs):
        rs = dict(errorcode=errorcode, errormsg=errormsg)
        for k, v in kwargs.items():
            rs[k] = v
        return json.dumps(rs)
