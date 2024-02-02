class Queries(object):
    """Database queries"""
    
    #1. Create a view for the ARC administrator called “Top_Machines_Used”. (25/25)
    query1 = "create view Top_Machines_Used as(select equipment.equipment_type as `Equipment Name` , count(distinct timestamp) as `Total Number Of Days Used`,count(distinct card_id) as `Number Of Unique Users Using Equipment`,rank() over(order by count(distinct card_id) desc)as `Rank` from usage_reading natural join equipment where timestamp between '2023-01-01 00:00:00' and '2023-06-30 00:00:00' group by equipment.equipment_type limit 15);"
    
    #2. Create a view “Machines_Used_By_Day_Of_Week”
    query2 = """
            CREATE VIEW Machines_Used_By_Day_Of_Week as 
            SELECT equipment.equipment_type as 'Equipment Name', dayname(usage_reading.timestamp) AS 'Day of Week', member_type AS 'Type of Member', 
            count(*) AS 'Count' FROM usage_reading JOIN equipment ON equipment.equipment_id = usage_reading.equipment_id 
            JOIN 
            (SELECT card_id, student_type AS member_type FROM student UNION SELECT card_id, member_type FROM non_student UNION SELECT card_id, 'Family' AS member_type FROM family) 
            AS member_class ON member_class.card_id = usage_reading.card_id GROUP BY equipment.equipment_type, member_type, dayname(usage_reading.timestamp) with rollup;
            """
    
    #3. Create a row level trigger that no update can reduce an employee salary.
    query3="""
            create trigger NoLowerSalary 
            before update on employee
            for each row
            begin
            if NEW.salary_hour < OLD.salary_hour then
		        set NEW.salary_hour = OLD.salary_hour;
            end if;
            end; 
            """
    
    #4. Create a tuple level check constraint that checks that all employees make atleast 12 dollars per hour
    query4="ALTER TABLE employee ADD CONSTRAINT chk_salary_range CHECK (salary_hour >= 12);"

    #5. Find the maximum length of supervisor employees for any employee of ARC?
    query5="""
            with recursive rec_employee(card_id, supervisor_card_id) as(select card_id, supervisor_card_id from employee union select rec_employee.card_id, employee.supervisor_card_id from employee, rec_employee where rec_employee.supervisor_card_id=employee.card_id)
            select max(maxx.job_length) as max_depth from (select count(supervisor_card_id)+1 as job_length from rec_employee natural join person group by card_id) as maxx;
            """

    #6. Find the 2nd youngest employee who earns the most salary in ARC
    query6="select ranked.name from (SELECT name,dob,salary_hour,DATEDIFF(CURRENT_DATE, dob) / 365 AS age, dense_rank() over(order by salary_hour desc,DATEDIFF(CURRENT_DATE, dob) / 365) as rankk  FROM person natural join employee order by salary_hour desc,age) as ranked where ranked.rankk=2 ;"
