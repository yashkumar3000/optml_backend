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

  filename = filename.split(".")[0].toLowerCase();
  filename = filename.replace(/[^0-9a-zA-Z$_]/g, "");
  
  file.mv(obj.uri, (err, result) => {
    if (err) console.log(err);
    console.log("successfully uploaded");
  });

  let options = {
    scriptPath: __dirname,
    args: [obj.uri, filename],
  };

  let table = "merge_cap_" + filename;
  
  let sql = `SHOW TABLES LIKE '${table}'`;

  db.query(sql, (err, result) => {
    if (err) console.log(err);

    if (result.length == 0) {
      PythonShell.run("rtopy.py", options,async (err, resu) => {
        if (err) {
          console.log("An error has occured  :(", err);
        }

        console.log(resu);
        
        if(resu[0].split(':')[0] == 'Error'){
          res.status(400).json({
            message:resu[0]
          })
        }
        else{
          res.status(200).json({
            uri: obj.uri,
            filename: "merge_cap_" + filename,
          });
        }

      });
    } else {
      console.log("Table already exists");
      res.status(200).json({
        uri: obj.uri,
        filename: "merge_cap_" + filename,
      });
    }
  });
  
});

//Sending Province and Devicetype to frontend
app.get("/filterNames/:file", (req, res) => {
  let file = req.params.file;

  let sql1 = `SELECT Distinct Province   from ${file}`;
  let sql2 = `SELECT Distinct DeviceType  from ${file}`;

  db.query(`${sql1};${sql2}`, (err, result) => {
    if (err) console.log(err);

    let results = {};
    results["Province"] = result[0];
    results["DeviceType"] = result[1];
    // console.log(result);
    res.status(200).json({ ...results });
  });
});

// Filter functionality
app.get("/data/:file", (req, res) => {
  let province = req.query.province || "all";
  let device = req.query.device || "all";
  let file = req.params.file;

  let sqlmap = `SELECT Lab,DeviceType,Province,Latitude,Longitude, ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by Province ,Lab,DeviceType `;
  // let sqlbar1 = `SELECT Province,DeviceType, sum(Hist_test) as histtest from ${file} group by Province,DeviceType `;

  let sqlbar2 = `SELECT Province,DeviceType,ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100)  as Capacity_Utilization from ${file} group by Province,DeviceType `;

  let suff = "";
  if (province != "all" || device != "all") {
    suff = "having " + (province != "all" ? `Province = '${province}' ` : "");
    if (suff == "having ") {
      suff += device != "all" ? `DeviceType = '${device}' ` : "";
    } else {
      suff += device != "all" ? `AND DeviceType = '${device}' ` : "";
    }
  }
  let sqlUtilCard;
  sqlmap = sqlmap + suff;
  sqlbar2 = sqlbar2 + suff;

  let sqlcap_prov;
  let sqlcap_dev;

  if (province == "all" && device != "all") {
    sqlcap_prov = `SELECT t.Province, t.Capacity_Utilization as Capacity_Utilization from (${sqlbar2}) as t`;
    sqlcap_dev =
      `SELECT DeviceType,ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by DeviceType ` +
      suff;
    sqlUtilCard = `SELECT ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} where DeviceType = '${device}'`;
  } else if (province != "all" && device == "all") {
    sqlcap_dev = `SELECT t.DeviceType, t.Capacity_Utilization as Capacity_Utilization from (${sqlbar2}) as t`;
    sqlcap_prov =
      `SELECT Province,ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by Province ` +
      suff;
    sqlUtilCard = `SELECT ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} where Province = '${province}'`;
  } else if (province == "all" && device == "all") {
    sqlcap_dev = `SELECT DeviceType,ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by DeviceType  `;
    sqlcap_prov = `SELECT Province,ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} group by Province `;
    sqlUtilCard =
      `SELECT ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} ` +
      suff;
  } else {
    sqlcap_prov = `SELECT t.Province, t.Capacity_Utilization as Capacity_Utilization from (${sqlbar2}) as t`;
    sqlcap_dev = `SELECT t.DeviceType, t.Capacity_Utilization as Capacity_Utilization from (${sqlbar2}) as t`;
    sqlUtilCard = `SELECT ROUND((sum(Hours_Consumed1)/sum(Annual_Hours1))*100) as Capacity_Utilization from ${file} where Province = '${province}' and DeviceType = '${device}'`;
  }

  db.query(
    `${sqlmap};${sqlcap_prov};${sqlcap_dev};${sqlUtilCard}`,
    (err, result) => {
      if (err) console.log(err);
      let results = {};

      results["map"] = result[0];
      results["provinceCapcityUtil"] = result[1];
      results["deviceCapacityUtil"] = result[2];
      results["sqlUtilCard"] = result[3];
      // res.send(JSON.stringify(result)) ;
      res.status(200).json({ ...results });
    }
  );
});

app.listen("3000", () => {
  console.log("Server Started...");
});
