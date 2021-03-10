SELECT EmptyCustomer.cid, EmptyCustomer.firstname, EmptyCustomer.gender, Cart.cartid, Cart.status
FROM EmptyCustomer,Cart
WHERE EmptyCustomer.cid=Cart.cid