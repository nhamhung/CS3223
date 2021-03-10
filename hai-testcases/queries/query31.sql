SELECT Customer.cid, Customer.firstname, Customer.gender, EmptyCart.cartid, EmptyCart.status
FROM Customer,EmptyCart
WHERE Customer.cid=EmptyCart.cid