const express = require("express");
const upload = require("express-fileupload");
const path = require("path");
const readXlsxFile = require("read-excel-file/node");
const multer = require("multer");
const { query } = require("express");
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

app.get("/", (req, res) => {
  res.send("Welcome to homepage... ");
});

// Login API
app.post("/login", (req, res) => {
  let sql = `select * from users where email = '${req.body.email}'`;

  let query = db.query(sql, (err, result) => {
    if (err) {
      console.log(err);
    }
    if (result.length == 0) {
      sql = `INSERT INTO users(email,firstName,lastName,password) values('${req.body.email}','${req.body.firstName}','${req.body.lastName}','${req.body.password}') `;
      db.query(sql, (err1, result1) => {
        if (err1) {
          console.log(err1);
        }
        console.log(result.insertId);
        res.send("Logged In(New User)");
      });
    } else {
      // res.send(result[0].email) ;
      res.send("Logged In..");
    }
  });
});

// To upload the file 

app.post("/upload/:uid", (req, res) => {
  let uid = req.params.uid;

  db.query("SELECT * FROM users where id = ?", [uid],async (err, resu) => {
    if (err) console.log(err);

    if (resu.length == 0) {
      res.send("No such user exists.");
    } else {
      let file = req.files.data; // (form-data object name should be data)4
      let filename = file.name.split(" ").join("_");
      let obj = {
        uid: uid,
        filetype: file.mimetype,
        filename: filename,
        uri: __dirname + "/uploads/" + filename,
      };

      await file.mv(obj.uri, (err, result) => {
        if (err) console.log(err);
        console.log("successfully uploaded");
      });

      let sql = `select * from files where uid = ? and filename = ?`;
      db.query(sql, [uid, obj.filename],  (err, result) => {
        if (err) console.log(err);

        if (result.length == 0) {
          // Converting the raw file to final file.
                
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
          
          //Deleting the raw file from uploads folder.
          // fs.unlinkSync(obj.uri);

          // renaming the merged file
          obj.filename = 'merge_'+obj.filename;
          obj.uri = __dirname + "/uploads/" + obj.filename;

          // Inserting file to database
          sql = "INSERT INTO files SET ?";  
          db.query(sql, obj, (err, insertRes) => {
            if (err) console.log(err);

            res.status(200).json({
              uri: obj.uri,
              filename:obj.filename
            });
          });
        } else {
          res.send(
            "File of this name already exists, either change the file name or upload new file"
          );
        }
      });
      // convertExcelFileToJsonUsingXlsx(obj.uri, filename);
    }
  });
});

async function importFileToDb(exFile) {
  await readXlsxFile(exFile).then((rows) => {
    rows.shift();
    let query =
      "INSERT INTO map (labname,device,histtest,hrsconsumed,annualhrs,provience,latitude,longitude) VALUES ?";
    db.query(query, [rows], (error, response) => {
      console.log("Inserted");
    });
  });
}


app.get("/map/:filename", async (req, res) => {
  // await importFileToDb(__dirname + '/uploads/' + req.params.filename) ;
  const json = JSON.parse(
    await readFile(new URL("./uploads/" + req.params.filename + ".json"))
  );
  console.log(json);
  // var query = 'SELECT *, (sum(hrsconsumed)/sum(annualhrs))*100 as utilization from map group by provience' ;

  // db.query(query , (error, result) => {
  //     if(error) console.log('an error has occured ');

  //     res.json(result);
  // })
});


app.get('/woosh' , (req , res)=>{
  var filePath = __dirname + '/uploads/Demand.csv'; 
  fs.unlinkSync(filePath);
  res.status(200).json({
    "message":"Done Successsfully.."
  })
});

app.get("/barchart1/:filename", async (req, res) => {
  await importFileToDb(__dirname + "/uploads/" + req.params.filename);
  var query =
    "SELECT provience , sum(histtest) as histtest from map group by provience";
  db.query(query, (error, result) => {
    if (error) console.log("an error has occured ");

    res.json(result);
  });
});

app.get("/barchart2/:filename", async (req, res) => {
  await importFileToDb(__dirname + "/uploads/" + req.params.filename);
  var query =
    "SELECT *, (sum(hrsconsumed)/sum(annualhrs))*100 as utilization from map group by provience";

  db.query(query, (error, result) => {
    if (error) console.log("an error has occured ");

    res.json(result);
  });
});

app.delete("/clearmap", (req, res) => {
  var query = "delete from map";
  db.query(query, (err, res) => {
    if (err) console.log(err);
  });
  res.send("successfully deleted");
});

function convertExcelFileToJsonUsingXlsx(exFile, filename) {
  // Read the file using pathname
  const file = xlsx.readFile(exFile);

  // Grab the sheet info from the file
  const sheetNames = file.SheetNames;
  const totalSheets = sheetNames.length;

  // Variable to store our data
  let parsedData = [];

  // Loop through sheets
  for (let i = 0; i < totalSheets; i++) {
    // Convert to json using xlsx
    const tempData = xlsx.utils.sheet_to_json(file.Sheets[sheetNames[i]]);

    // Skip header row which is the colum names
    tempData.shift();

    // Add the sheet's json to our data array
    parsedData.push(...tempData);
  }

  // call a function to save the data in a json file

  generateJSONFile(parsedData, filename);
}

function generateJSONFile(data, filename) {
  try {
    fs.writeFileSync(
      `${__dirname}/uploads/${filename}.json`,
      JSON.stringify(data)
    );
  } catch (err) {
    console.error(err);
  }
}

app.listen("3000", () => {
  console.log("Server Started...");
});
