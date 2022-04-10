# OZON-reports-py
Use python script to get information about comissions to every order (for ozon seller)

Method: https://api-seller.ozon.ru/v3/finance/transaction/list

Information from API send to MSSQL server in 3 tables: Main, Services, Items

- Main table include information about comissions from OZON for every order
- Services table include another comissions 
- Items include information about items in every order.

For use this script, you need:
1. API-key (from ozon seller)
2. Client ID (from ozon seller)
3. MSSQL Server name, db name, login and password
4. Date period (star, end)
