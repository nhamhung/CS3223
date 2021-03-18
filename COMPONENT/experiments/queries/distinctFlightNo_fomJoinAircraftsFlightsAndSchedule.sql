SELECT DISTINCT Flights.flno
FROM Flights,Schedule,Aircrafts
WHERE Flights.flno=Schedule.flno,Schedule.aid=Aircrafts.aid
ORDERBY Flights.flno,Schedule.aid