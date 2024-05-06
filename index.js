import 'dotenv/config';
import { config, dbInfos } from './load-config.js';
import logger from './logger.js';

// express - Lightweight node.js framework for web application/APIs
import express from 'express'; // global variable express
import cors from 'cors'; // cors - Express middleware for handling http requests
import bodyParser from 'body-parser';
import path from 'path'; // Path directory variable

function reqLogger(req, res, next) {
  console.log("Request Method: ", req.method);
  console.log("Request URL: ", req.url);
  next();
}

const app = express();
app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(express.static(path.resolve("./public")));

// customize middleware, for demo purpose only
app.use(reqLogger);

// import modules
import apiRouter from './modules/api.js';

app.use('/server/api', apiRouter);


// ------------------------------------------------------------------
// ----------------- Listen for incoming requests -------------------
// ------------------------------------------------------------------


app.listen(config.API_PORT, '0.0.0.0', () => logger.info(`LISTENING ON PORT ${config.API_PORT}`));

import https from 'https';
import fs from 'fs';
const options = {
}

const httpsServer = https.createServer(options, app);
httpsServer.listen(config.HTTPS_PORT,'0.0.0.0', () => logger.info(`HTTPs LISTENING ON PORT ${config.HTTPS_PORT}`));

