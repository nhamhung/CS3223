SELECT EmptyCustomer.cid, EmptyCustomer.firstname, EmptyCustomer.gender, EmptyCart.cartid, EmptyCart.status
FROM EmptyCustomer,EmptyCart
WHERE EmptyCustomer.cid=EmptyCart.cid