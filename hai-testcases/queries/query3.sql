SELECT DISTINCT Customer.cid, Customer.firstname, Customer.gender, Cart.cartid, Cart.status, Cart.cid
FROM Customer,Cart
WHERE Customer.cid=Cart.cid, Customer.cid=Cart.cartid
ORDERBY Cart.cartid
