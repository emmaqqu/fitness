from flask import Flask, render_template
from db import init_db, check_login, create_user
app = Flask(__name__)
init_db()

@app.route("/")
def index():
    return "<p>Welcome!</p>"

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        user = check_login(username, password)

        if user:
            return redirect("/home")
        else:
            return render_template("login.html", error="Invalid credentials")

    return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        create_user(username, password)
        return redirect("/login")

    return render_template("register.html")

if __name__ == '__main__':
    app.run(debug=True)