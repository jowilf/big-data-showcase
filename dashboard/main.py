import threading
from confluent_kafka import Consumer
from dash import Dash, html, dcc, Output, Input

from dashboard.event import RealTimeEvent
from dashboard.hbase import HBaseReader
from dashboard.config import KAFKA_URL, KAFKA_GROUP_ID, THREAD_DAEMON


import flask

server = flask.Flask(__name__)

app = Dash(__name__, server=server)

event = RealTimeEvent()
hbase = HBaseReader()


app.layout = html.Div(
    className="row",
    children=[
        html.H1(children="DashBoard", style={"textAlign": "center"}),
        html.Div(
            children=[
                dcc.Graph(figure=HBaseReader().figure(), id="brand-sales"),
            ],
        ),
        html.Div(
            children=[
                dcc.Graph(id="graph-content"),
            ],
        ),
        dcc.Interval(
            id="interval-component",
            interval=1 * 1000,
            n_intervals=0,  # in milliseconds
        ),
    ],
)


@app.callback(
    Output("graph-content", "figure"), Input("interval-component", "n_intervals")
)
def update_graph_live(n):
    event.next()
    return event.figure()


def kafka_listener():
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_URL,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe(["electronic-analytics"])
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        key = msg.key().decode("utf-8") if msg.key() is not None else ""
        value = msg.value().decode("utf-8")

        print("Received message: {} ;; {}".format(key, value))

        if key == "event_type_agg":
            event.add_event(value)


thread = threading.Thread(name="kafka consumer", target=kafka_listener)
thread.setDaemon(THREAD_DAEMON)
thread.start()

if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=True, host="0.0.0.0", port=8051)
