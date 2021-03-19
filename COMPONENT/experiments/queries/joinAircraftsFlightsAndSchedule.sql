SELECT Schedule.flno,Schedule.aid,Flights.flno,Flights.from,Flights.to,Flights.distance,Flights.departs,Flights.arrives,Aircrafts.aid,Aircrafts.aname,Aircrafts.cruisingrange
FROM Flights,Schedule,Aircrafts
WHERE Flights.flno=Schedule.flno,Schedule.aid=Aircrafts.aid
ORDERBY Flights.flno,Schedule.aid