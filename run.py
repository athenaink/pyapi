# -*- coding: utf-8 -*-
# @Time    : 2019/6/21 10.08
# @Author  : KEN.LI

from flask import Flask, request, jsonify
import os
from piquery.piquery import PIQBuilder
from piquery.piquery import Response

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['PORT'] = int(os.environ['KDD_PORT']) if 'KDD_PORT' in os.environ else 5000

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/piq_repeat", methods=["POST", "GET"])
def piq_repeat():
    if request.method=='POST':
        form = request.json
        url = form["url"]
        piq = PIQBuilder.build()
        return piq.queryRepeat(url)
    elif request.method=='GET':
        args = request.args
        url = args["url"]
        piq = PIQBuilder.build()
        return piq.queryRepeat(url)
    return jsonify(errorcode=404)

@app.route("/piq_repeat_case", methods=["POST", "GET"])
def piq_repeat_case():
    if request.method=='POST':
        # form = request.form
        form = request.json
        url = form['url']
        case_ids = form['case_ids']
        piq = PIQBuilder.buildCase(case_ids=case_ids)
        return piq.queryRepeat(url)
    return jsonify(errorcode=404)

@app.route("/piq_add", methods=["POST", "GET"])
def piq_add():
    if request.method=='POST':
        # form = request.form
        form = request.json
        cid = form['cid']
        _id = form['id']
        url = form['url']

        cmd = PIQBuilder.buildAddCmd(cid, _id, url)
        cmd.execute()
        return jsonify(errorcode=0, errormsg='ok')
    return jsonify(errorcode=404)

@app.route("/piq_del", methods=["POST", "GET"])
def piq_del():
    if request.method=='POST':
        # form = request.form
        form = request.json
        cid = form['cid']
        _id = form['id']

        cmd = PIQBuilder.buildDelCmd(cid, _id)
        cmd.execute()
        return jsonify(errorcode=0, errormsg='ok')
    return jsonify(errorcode=404)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=app.config['PORT'], threaded=True)
