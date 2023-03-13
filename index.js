const fs = require("fs");
const { Transform } = require("stream");
const csv = require("csvtojson");
const { S3 } = require("@aws-sdk/client-s3");
const s3 = new S3({ region: "ap-south-1" });
const inputStream = (path = "sample-data.csv") => fs.createReadStream(path);
const chain = require("stream-chain");
const csvParser = csv();

const { randomUUID } = require("crypto");
const config = require("./config");

let buffer = [];
const transformStream = new Transform({
  objectMode: true,
  transform(chunk, encoding, callback) {
    // let cnt = 0;
    chunk = JSON.parse(chunk.toString());

    chunk = {
      id: chunk.id,
      firstName: chunk.firstName,
      lastName: chunk.lastName,
      emailBusiness: chunk.email2,
      status: "Success",
    };
    buffer.push(chunk);
    if (buffer.length >= 50) {
      if (this.push(buffer.splice(0))) {
        callback();
      }
    } else {
      callback();
    }
  },
  final(callback) {
    if (buffer.length !== 0) this.push(buffer);
    callback();
  },
});

const transformStream2 = new Transform({
  objectMode: true,
  transform(chunk, encoding, callback) {
    chunk = chunk.reduce(function (r, a, index) {
      r[a.status] = r[a.status] || [];
      r[a.status].push(a);
      delete r.Success[index].status;
      return r;
    }, {});

    this.push(chunk);
    callback();
  },
});

// function upload() {
//   const pass = new PassThrough();

//   //   console.log(pass);
//   const parallelUpload = new Upload({
//     client: s3,
//     params: {
//       Bucket: "ra-sample-bucket",
//       Key: "sample-data.json",
//       Body: pass,
//       ContentType: "application/json",
//     },
//     queueSize: 4,
//     partSize: 1024 * 1024 * 5,
//     leavePartsOnError: false,
//   });
//   parallelUpload.on("httpUploadProgress", (progress) => {
//     console.log(progress);
//   });
//   parallelUpload
//     .done()
//     .then((res) => {
//       console.log(res);
//     })
//     .catch((err) => {
//       console.log(err);
//     });
//   return pass;
// }

const pipe = new chain([
  inputStream("sample-data.csv"),
  csvParser,
  transformStream,
  transformStream2,
]);

pipe.on("data", async (chunk) => {
  // console.log(chunk);

  const sql =
    "INSERT INTO newtable (id, firstName, lastName, emailBusiness) VALUES ?";
  var values = [
    chunk.Success.map((item) => [
      item.id,
      item.firstName,
      item.lastName,
      item.emailBusiness,
    ]),
  ];
  config.connection.query(sql, values, async function (err) {
    if (err) throw err;

    console.log("DONE!");
  });
  await s3.putObject({
    ...config.params,
    Key: `${randomUUID()}.json`,
    Body: JSON.stringify(chunk.Success),
    ContentType: "application/json",
  });
  // process.exit(0);
});

// const sql = `LOAD DATA LOCAL INFILE "sample-data.csv" INTO TABLE newtable  FIELDS TERMINATED BY ','
//   ENCLOSED BY '"'
//    LINES TERMINATED BY '\r\n'
//     IGNORE 1 ROWS`;
// pipe.on("end", () => {
//   process.exit(0);
// })
