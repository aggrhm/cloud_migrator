const fs = require('fs');
const YAML = require('yaml');
const SwiftClient = require('openstack-swift-client');
const AWS = require('aws-sdk');

const argv = require('yargs')
  .usage('Usage: $0 --config-path [config_path] --op [op]')
  .demandOption(['config-path', 'op'])
  .argv;

const TEMP_FILE_PATH = "/tmp/cloud_migrator_cache_file";

//console.log(argv.configPath);
const configStr = fs.readFileSync(argv.configPath, 'utf8');
console.log(configStr);
const config = YAML.parse(configStr);

async function migrate() {
  if (argv.op == 'cephToS3') {
    await migrateCephObjectStorage();
  } else {
    console.error("Op unknown.");
  }
};

async function migrateCephObjectStorage() {
  // connect to swift
  var swiftAuth = new SwiftClient.KeystoneV3Authenticator({
    endpointUrl: config.swift.keystone_url,
    username: config.swift.username,
    password: config.swift.password,
    domainId: config.swift.domain_id,
    projectId: config.swift.tenant_id
  });
  var swiftClient = new SwiftClient(swiftAuth);
  var containerName = config.swift.container;
  var cntr = swiftClient.container(containerName);

  // connect to s3
  var s3Client = new AWS.S3({
    apiVersion: '2006-03-01',
    region: config.s3.region,
    accessKeyId: config.s3.access_key_id,
    secretAccessKey: config.s3.secret_access_key
  });
  var s3BucketName = config.s3.bucket;

  // list all files
  console.log("Files");
  var files = await cntr.list(null, {limit: 10000, prefix: 'image/'});
  console.log(files);
  
  // copy each file
  for (var file of files) {
    var fst = fs.createWriteStream(TEMP_FILE_PATH);
    // download file
    console.log(`Downloading file ${file.name}`);
    await cntr.get(file.name, fst);

    // copy file to s3
    console.log("Uploading file to s3");
    var rfst = fs.createReadStream(TEMP_FILE_PATH);
    rfst.on('error', (err)=> {
      console.error('File error', err)
    });
    var uparams = {
      Bucket: config.s3.bucket,
      Key: file.name,
      Body: rfst
    };
    if (config.s3.public) {
      uparams.ACL = 'public-read';
    }
    await s3Client.upload(uparams).promise();

    // delete temp file
    console.log("Deleting temp file");
    fs.unlinkSync(TEMP_FILE_PATH);
  }
}

migrate().then((res)=> {
  console.log("Done.");
}).catch((err)=> {
  console.error(err);
})