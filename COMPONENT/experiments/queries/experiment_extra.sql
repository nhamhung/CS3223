SELECT DISTINCT Employees.eid,Employees.ename,Employees.salary
FROM Employees,Certified,Schedule
WHERE Employees.eid=Certified.eid,Certified.aid=Schedule.aid