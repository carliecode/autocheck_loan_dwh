use autocheck_db
go

with missed_payments as
(
	select
	 	ps.loan_id, ps.expected_payment_date, ps.expected_payment_amount
	from silver.payment_schedules ps
	left join silver.loan_payments lp 
	on ps.loan_id = lp.loan_id and ps.expected_payment_date = lp.date_paid and expected_payment_amount = amount_paid
	where amount_paid is null
),
cummulative_debts as
(
	select 
		loan_id,
		expected_payment_date,
		expected_payment_amount amount_due,
		datediff(day,expected_payment_date, getdate()) days_overdue,
		sum(expected_payment_amount) over (partition by loan_id order by expected_payment_date) amount_due_to_date,
		sum(datediff(day, expected_payment_date ,getdate())) over (partition by loan_id order by expected_payment_date) days_overdue_to_date
	from  missed_payments
)
select
	loan_id,
	max(days_overdue) days_overdue,
	max(amount_due_to_date) amount_overdue
from cummulative_debts
group by loan_id

