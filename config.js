const dotenv = require("dotenv").config();
const mysql = require("mysql2");
console.log(process.env.USERNAME);
const connection = mysql.createConnection({
  host: process.env.HOST,
  user: process.env.DB_USER,
  database: process.env.DATABASE,
  password: process.env.PASSWORD,
});

const params = { Bucket: process.env.S3_BUCKET };
module.exports = { connection, params };
