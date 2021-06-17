import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///./Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/><br/>"
        f"/api/v1.0/stations<br/><br/>"
        f"/api/v1.0/tobs<br/><br/>"
        f"/api/v1.0/start_date</br>"
        f"/api/v1.0/start_date/end_date"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return precipitation data"""
    # Query precipitation measurments
    results = session.query(Measurement.date, Measurement.prcp).all()

    session.close()

    # Convert list of tuples into normal list
    results = list(np.ravel(results))

    return jsonify(dict(results))

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return list of stations and station names"""
    # Query stations and station names
    results = session.query(Station.station, Station.name).join(Measurement, Station.station == Measurement.station)

    session.close()

    return jsonify(dict(results))

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return temperature observations from the most active station for the most recent one-year period"""
    
    # Find the most recent date in the data set.
    most_recent_date_str = session.query(func.max(Measurement.date)).first()[0]

    # Convert string date to datetime date
    most_recent_date = dt.datetime.strptime(most_recent_date_str, "%Y-%m-%d").date()
    
    # Calculate the date one year from the last date in data set.
    year_prior = most_recent_date - dt.timedelta(days=365)
    
    # Find the station with the greatest number of measurements
    most_active_station = session.query(Measurement.station, func.count(Measurement.id)).group_by(Measurement.station).order_by(func.count(Measurement.id).desc()).first()[0]

    # Query temperature observations for most recent year
    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= year_prior, Measurement.station == most_active_station)

    session.close()

    return jsonify(dict(results))

@app.route("/api/v1.0/<start>")
def summarize_temp_after_date(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    most_recent_date_str = session.query(func.max(Measurement.date)).first()[0]

    # Convert string date to datetime date
    most_recent_date = dt.datetime.strptime(most_recent_date_str, "%Y-%m-%d").date()

    """Return min, max, and average temperatures after specifified date"""
    # Format start date as datetime date
    start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
    
    # Query data
    results = session.query(func.max(Measurement.tobs).label('temp_max'), func.min(Measurement.tobs).label('temp_min'), func.avg(Measurement.tobs).label('temp_avg')).filter(Measurement.date >= start_date).all()

    session.close()
    
    # Create a dictionary from the data
    temps_list = []
    for temp_max, temp_min, temp_avg in results:
        temps_dict = {}
        temps_dict['start_date'] = str(start_date)
        temps_dict['end_date'] = str(most_recent_date)
        temps_dict["temp_max"] = temp_max
        temps_dict["temp_min"] = temp_min
        temps_dict["temp_avg"] = temp_avg
        temps_list.append(temps_dict)

    return jsonify(temps_list)

@app.route("/api/v1.0/<start>/<end>")
def summarize_temp_between_dates(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return min, max, and average temperatures between specifified dates (inclusive)"""
    # Format start date as datetime date
    start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
    end_date = dt.datetime.strptime(end, "%Y-%m-%d").date()
    
    # Query data
    results = session.query(func.max(Measurement.tobs).label('temp_max'), func.min(Measurement.tobs).label('temp_min'), func.avg(Measurement.tobs).label('temp_avg')).filter(Measurement.date >= start_date, Measurement.date <= end_date).all()

    session.close()
    
    # Create a dictionary from the data
    temps_list = []
    for temp_max, temp_min, temp_avg in results:
        temps_dict = {}
        temps_dict["start_date"] = str(start_date)
        temps_dict["end_date"] = str(end_date)
        temps_dict["temp_max"] = temp_max
        temps_dict["temp_min"] = temp_min
        temps_dict["temp_avg"] = temp_avg
        temps_list.append(temps_dict)

    return jsonify(temps_list)

if __name__ == '__main__':
    app.run(debug=True)