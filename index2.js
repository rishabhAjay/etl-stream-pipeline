const etl = require("etl");
const fs = require("fs");

const { S3 } = require("@aws-sdk/client-s3");
const s3 = new S3({ region: "ap-south-1" });
const chain = require("stream-chain");

const { randomUUID } = require("crypto");
const config = require("./config");
const { Transform, Writable } = require("stream");

const transformStream2 = new Writable({
  objectMode: true,
  writev(chunk, callback) {
    console.log(chunk);
    config.connection.query(
      "CREATE TEMPORARY TABLE IF NOT EXISTS temporary_table SELECT * FROM newtable LIMIT 0;",
      function (err) {
        if (err) throw err;
        // conn.end();
        var values = [
          chunk[0].chunk.Success.map((item) => [
            item.id,
            item.firstName,
            item.lastName,
            item.emailBusiness,
          ]),
        ];
        // console.log(values);
        config.connection.query(
          "INSERT INTO temporary_table (id, firstName, lastName, emailBusiness) VALUES ?",
          values,
          function (err) {
            if (err) throw err;
            callback();

            // conn.end();
          }
        );
        // config.connection.end();

        // process.exit(0);
      }
    );
    // console.log(chunk);
    // return chunk;
    // pipe.emit("finish");
  },
});

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
  transformStream2.on("finish", () => {
    config.connection.query(
      `select * from temporary_table;`,

      function (err, results) {
        if (err) throw err;
        console.log(results);
        // conn.end();
        console.log("DONE!");
      }
    );
    config.connection.query(
      `INSERT IGNORE INTO newtable select * from temporary_table;`,

      function (err) {
        if (err) throw err;
        // conn.end();
        config.connection.end();

        console.log("DONE!");
        process.exit(0);
      }
    );
  }),
]);

// pipe.on("data", async (chunk) => {
//   console.log(chunk);
// });

// pipe.pipe(transformStream2);
