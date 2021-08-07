# doing necessary imports
import threading
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

from flask import Flask, render_template, request, jsonify, Response, url_for, redirect
from flask_cors import CORS, cross_origin
import pandas as pd
from mongoDBOperations import MongoDBManagement
from FlipkratScrapping import FlipkratScrapper
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import configHandler as cfg
import cassandraOps ,customLogger as lgr

rows = {}
collection_name = None

free_status = True

app  = Flask(__name__)  # initialising the flask app with the name 'app'
clg  = lgr.customLogger(__name__)
ch   = cfg.configHandler("config.ini")

# Reading Config properties
mongoOptions = ch.readConfigSection("mongodb")
db_name    = mongoOptions['db_name']
mongoUsr   = mongoOptions['user']
mongoPwd   = mongoOptions['passwd']

output_folder = ch.readConfigOptions("output", "directory")

#For selenium driver implementation on heroku
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("disable-dev-shm-usage")

#To avoid the time out issue on heroku
class threadClass:

    def __init__(self, expected_review, searchString, scrapper_object, review_count):
        self.expected_review = expected_review
        self.searchString = searchString
        self.scrapper_object = scrapper_object
        self.review_count = review_count
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        global collection_name, free_status
        free_status = False
        collection_name = self.scrapper_object.getReviewsToDisplay(expected_review=self.expected_review,
                                                                   searchString=self.searchString, username=mongoUsr, password=mongoPwd,
                                                                   review_count=self.review_count)
        clg.log("Thread run completed")
        free_status = True


@app.route('/', methods=['POST', 'GET'])
@cross_origin()
def index():
    if request.method == 'POST':
        global free_status
        ## To maintain the internal server issue on heroku
        if free_status != True:
            return "This website is executing some process. Kindly try after some time..."
        else:
            free_status = True
        searchString = request.form['content'].replace(" ", "")  # obtaining the search string entered in the form
        expected_review = int(request.form['expected_review'])

        try:
            review_count = 0
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options)

            scrapper_object.openUrl("https://www.flipkart.com/")
            clg.log("Url hitted")
            scrapper_object.login_popup_handle()
            clg.log("login popup handled")
            scrapper_object.searchProduct(searchString=searchString)
            clg.log(f"Search begins for {searchString}")

            mongoClient = MongoDBManagement(username=mongoUsr, password=mongoPwd)
            if mongoClient.isCollectionPresent(collection_name=searchString, db_name=db_name):
                response = mongoClient.findAllRecords(db_name=db_name, collection_name=searchString)
                reviews = [i for i in response]
                if len(reviews) > expected_review:
                    result = [reviews[i] for i in range(0, expected_review)]
                    scrapper_object.saveDataFrameToFile(file_name=output_folder + "/"+searchString+"_data.csv",
                                                        dataframe=pd.DataFrame(result))
                    clg.log("Data saved in scrapper file")
                    return render_template('results.html', rows=result)  # show the results to user
                else:
                    review_count = len(reviews)
                    threadClass(expected_review=expected_review, searchString=searchString,
                                scrapper_object=scrapper_object, review_count=review_count)
                    clg.log("data saved in scrapper file")
                    return redirect(url_for('feedback'))
            else:
                threadClass(expected_review=expected_review, searchString=searchString, scrapper_object=scrapper_object,
                            review_count=review_count)
                return redirect(url_for('feedback'))

        except Exception as e:
            # raise Exception("(app.py) - Something went wrong while rendering all the details of product.\n" + str(e))
            msg = "(app.py) - Something went wrong while rendering all the details of product.\n" + str(e)
            clg.log(msg,"ERROR")

    else:
        return render_template('index.html')


@app.route('/feedback', methods=['GET'])
@cross_origin()
def feedback():
    try:
        global collection_name
        if collection_name is not None:
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options)
            mongoClient = MongoDBManagement(username=mongoUsr, password=mongoPwd)
            rows = mongoClient.findAllRecords(db_name=db_name, collection_name=collection_name)
            reviews = [i for i in rows]
            dataframe = pd.DataFrame(reviews)
            scrapper_object.saveDataFrameToFile(file_name=output_folder + "/scrapper_data.csv", dataframe=dataframe)
            collection_name = None
            return render_template('results.html', rows=reviews)
        else:
            return render_template('results.html', rows=None)
    except Exception as e:
        # raise Exception("(feedback) - Something went wrong on retrieving feedback.\n" + str(e))
        msg = "(feedback) - Something went wrong on retrieving feedback.\n" + str(e)
        clg.log(msg, "ERROR")


@app.route("/graph", methods=['GET'])
@cross_origin()
def graph():
    return redirect(url_for('plot_png'))


@app.route('/a', methods=['GET'])
def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


def create_figure():
    data = pd.read_csv("static/scrapper_data.csv")
    dataframe = pd.DataFrame(data=data)
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    xs = dataframe['product_searched']
    ys = dataframe['rating']
    axis.scatter(xs, ys)
    return fig

if __name__ == "__main__":
    app.run()  # running the app on the local machine on port 8000
