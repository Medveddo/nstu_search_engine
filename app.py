import time
from flask import Flask, render_template, redirect, request
import glob
from test_distance_score import SearchProvider


app = Flask(__name__, template_folder=".")


@app.get("/")
def index():
    return """
    <form action="/get_results">
        <label for="query">Search query:</label><br>
        <input type="text" id="query" name="query" placeholder="локоть новосибирск"><br>
        <input type="submit" value="Search">
    </form> 
    """


@app.get("/get_results")
def get_results():
    query = request.args.get("query")
    SearchProvider.search(query)
    return redirect("/results")


@app.get("/results")
def results():
    file_names = glob.glob("result_*")
    if not file_names:
        html = "<h1>Not found</h1>"
        return html
    html = ""
    file_names = sorted(file_names)
    for name in file_names:
        html += f'<a href="/{name}">{name}</a><br>'
    return html


@app.get("/<filename>")
def render(filename: str):
    return render_template(filename)


if __name__ == "__main__":
    app.run()
