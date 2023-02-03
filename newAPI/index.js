const express = require("express");
const upload = require("express-fileupload");
const path = require("path");
const readXlsxFile = require("read-excel-file/node");
const multer = require("multer");
const { query, json } = require("express");
const { type } = require("os");
const xlsx = require("xlsx");
const fs = require("fs");
const { PythonShell } = require("python-shell");
const db = require("./database/db");

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(upload());
app.use(express.static(path.resolve("./uploads")));

// Task - Will try to delete the file at uploads.:)

app.post("/upload", (req, res) => {
  let file = req.files.data; // (form-data object name should be data)
  let filename = file.name.split(" ").join("_");
  let obj = {
    filetype: file.mimetype,
    filename: filename,
    uri: __dirname + "/uploads/" + filename,
  };

  file.mv(obj.uri, (err, result) => {
    if (err) console.log(err);
    console.log("successfully uploaded");
  });

  let options = {
    scriptPath: __dirname,
    args: [obj.uri, obj.filename],
  };

  PythonShell.run("rtopy.py", options, (err, res) => {
    if (err) {
      console.log("An error has occured  :(", err);
    }
    console.log(res);
  });

  // renaming the merged file
  obj.filename = "merge_" + obj.filename;
  obj.uri = __dirname + "/uploads/" + obj.filename;

  res.status(200).json({
    uri: obj.uri,
    filename: "merge_cap_" + obj.filename.split(".")[0],
  });
});

app.get("/map/:file", (req, res) => {
  let file = req.params.file;
  db.query(
    `SELECT Lab,DeviceType,Province,Latitude,Longitude, ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by Province ,Lab,DeviceType`,
    (err, result) => {
      if (err) console.log(err);

      res.send(JSON.stringify(result));
    }
  );
});

app.get("/provinceHist/:file", (req, res) => {
  let file = req.params.file;

  db.query(
    `SELECT Province,sum(Hist_test) as histtest from ${file} group by Province`,
    (err, result) => {
      if (err) console.log(err);

      res.send(JSON.stringify(result));
    }
  );
});

app.get("/deviceHist/:file", (req, res) => {
  let file = req.params.file;
  db.query(
    `SELECT DeviceType,sum(Hist_test) as histtest from ${file} group by DeviceType`,
    (err, result) => {
      if (err) console.log(err);

      res.send(JSON.stringify(result));
    }
  );
});

app.get("/provinceCapacityUtil/:file", (req, res) => {
  let file = req.params.file;
  db.query(
    `SELECT Province, ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by Province `,
    (err, result) => {
      if (err) console.log(err);
      let resp = JSON.stringify(result);
      res.send(resp);
    }
  );
});


app.get("/deviceCapacityUtil/:file", (req, res) => {
  let file = req.params.file;
  db.query(
    `SELECT DeviceType, ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by DeviceType `,
    (err, result) => {
      if (err) console.log(err);

      res.send(JSON.stringify(result));
    }
  );
});

// Filter functionality
app.get("/data/:file", (req, res) => {
  let province = req.query.province || "all";
  let device = req.query.device || "all";
  let file = req.params.file;

  let sqlmap = `SELECT Lab ,DeviceType,Province,Latitude,Longitude, ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by Province `;
  let sqlbar1 = `SELECT Province, sum(Hist_test) as histtest from ${file} group by Province `;
  let sqlbar2 = `SELECT DeviceType, sum(Hist_test) as histtest from ${file} group by DeviceType `;

  let suff = "";
  if (province != "all" || device != "all") {
    suff = "having " + (province != "all" ? `Province = '${province}' ` : "");
    if (suff == "having ") {
      suff += device != "all" ? `DeviceType = '${device}' ` : "";
    } else {
      suff += device != "all" ? `AND DeviceType = '${device}' ` : "";
    }
  }
  sqlmap = sqlmap + suff;
  sqlbar1 = sqlbar1 + suff;
  sqlbar2 = sqlbar2 + suff;

  // db.query(`${sqlbar1};${sqlbar2};${sqlmap}` , (err ,result)=> {
  //   if(err) console.log(err) ;
  //   // let data = {};

  //   // data['map'] = JSON.stringify(result[0]);
  //   // data['bar1'] = JSON.stringify(result[1]);
  //   // data['bar2'] = JSON.stringify(result[2]);
  //   console.log(result);
  //   // res.send(data) ;
  // })

  db.query(sqlmap, (err, resu) => {
    if (err) console.log(err);

    console.log(resu);
  });

  res.send();
});

// const Mysql = require('mysql2/promise');

// const sql = Mysql.createPool({
//     host: 'localhost',
//     user: 'root',
//     password: 'yash2000',
//     database: 'optml',
//     waitForConnections: true,
//     connectionLimit: 100,
//     queueLimit: 0
// });

// // getting tx pending list from db
// async function getdata() {
//     try {
//         const query = "SELECT * FROM transactions WHERE `state` = 'pending'";
//         const rows = await sql.query(query);
//         return rows[0];
//     } catch (err) {
//         console.log('ERROR => ' + err);
//         return err;
//     }
// }

app.listen("3000", () => {
  console.log("Server Started...");
});
