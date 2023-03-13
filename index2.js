const etl = require("etl");
const fs = require("fs");

const { S3 } = require("@aws-sdk/client-s3");
const s3 = new S3({ region: "ap-south-1" });
const chain = require("stream-chain");
const { randomUUID } = require("crypto");
const config = require("./config");

const pipe = new chain([
  fs.createReadStream("sample-data.csv"),
  // parse the csv file
  etl.csv(),
  // map `date` into a javascript date and set unique _id
  etl.map((d) => {
    return {
      id: d.id,
      firstName: d.firstName,
      lastName: d.lastName,
      emailBusiness: d.email2,
      status: "Success",
    };
  }),
  etl.collect(50),
  etl.map((d) => {
    // console.log(d);
    return d.reduce(function (r, a, index) {
      r[a.status] = r[a.status] || [];
      r[a.status].push(a);
      delete r.Success[index].status;
      return r;
    }, {});
  }),
]);

pipe.on("data", async (chunk) => {
  console.log(chunk);

  const sql =
    "INSERT INTO newtable (id, firstName, lastName, emailBusiness) VALUES ?";
  var values = [
    chunk.map((item) => [
      item.id,
      item.firstName,
      item.lastName,
      item.emailBusiness,
    ]),
  ];
  config.connection.query(sql, values, function (err) {
    if (err) throw err;
    // conn.end();
    console.log("DONE!");
    process.exit(0);
  });
  await s3.putObject({
    ...config.params,
    Key: `${randomUUID()}.json`,
    Body: JSON.stringify(chunk),
    ContentType: "application/json",
  });
});
