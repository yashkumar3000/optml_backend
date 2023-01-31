const mysql = require('mysql') ;


const db = mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : 'yash2000',
    database : 'optml'
});

module.exports = db ;