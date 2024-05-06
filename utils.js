const config = require('./config');
const axios = require('axios');
const nodemailer = require("nodemailer");

// send email by remote ssh
function sendEmailbySSH(deliver_address, subject, message, cclist, callback) {
    const Client = require('ssh2').Client;
    var mail_command = 'echo "' + message + '" | mailx -s "' +'$(echo  "'+ subject + '\nContent-Type: text/html; charset-utf-8"' +')" '  + ' -c "' + cclist
        + '" -r "Support <' + config.SUPPORT_EMAIL + '>" "' + deliver_address + '"';
    var conn = new Client();
    conn.on('ready', () => {
        console.log('Client :: ready');
        conn.exec(mail_command, function (err, stream) {
            if (err) {
                console.error(err);
                callback(err, -1);
            }
            stream.on('close', function (code, signal) {
                console.log('Stream :: close :: code: ' + code + ', signal: ' + signal);
                conn.end();
                callback(null, code);
            }).on('data', function (data) {
                console.log('STDOUT: ' + data);
            }).stderr.on('data', function (data) {
                console.error('STDERR: ' + data);
            });
        });
    }).on('error', err => {
        console.error(err)
        callback(err, -1);
    }).connect({
        host: config.EMAIL_HOST,
        port: 22,
        username: config.EMAIL_USER,
        privateKey: require('fs').readFileSync('./.cert/id_rsa')
    });
}

// compare time of date only (HH:mm:ss)
// return: 1 - time2 > time1, 0 - time2 = time1, -1 - time2 < time1
function compareTime(time1, time2) {
    var hour1 = time1.getHours();
    var minute1 = time1.getMinutes();
    var second1 = time1.getSeconds();
    var hour2 = time2.getHours();
    var minute2 = time2.getMinutes();
    var second2 = time2.getSeconds();

    if (hour2 > hour1)
        return 1;
    else if (hour2 == hour1 && minute2 > minute1)
        return 1;
    else if (hour2 == hour1 && minute2 == minute1 && second2 > second1)
        return 1;
    else if (hour2 == hour1 && minute2 == minute1 && second2 == second1)
        return 0;
    else
        return -1;
}

function getCurISOTime() {
    let curTime = new Date();

    // current day
    // adjust 0 before single digit day
    let day = ("0" + curTime.getDate()).slice(-2);

    // current month
    let month = ("0" + (curTime.getMonth() + 1)).slice(-2);

    // current year
    let year = curTime.getFullYear();

    // current hours
    let hours = ("0" + curTime.getHours()).slice(-2);

    // current minutes
    let minutes = ("0" + curTime.getMinutes()).slice(-2);

    // current seconds
    let seconds = ("0" + curTime.getSeconds()).slice(-2);

    // current milliseconds
    let milliseconds = ("00" + curTime.getMilliseconds()).slice(-3);

    // return date & time in YYYY-MM-DD HH:MM:SS.ssssss format
    let cur_timestamp = year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds + "." + milliseconds;
    return cur_timestamp;
}

async function sendSsoRequest({ url, method='GET', headers={}, body={} }) {
    // Header
    const options = {
      url: url,
      method,
      timeout: 60000, // 1 minute timeout
      headers: headers,
      data: body
    };
    try {
      const response = await axios.request(options)
      if (response.status != 200) {
        console.error(`API ${url} respond error, response status: ${response.status}, message: ${response.data.error_description}`);
        return { "status": response.status, "message": response.data.error_description, "data": [] };
        }
        else {
            return {"status": 0, "data": response.data};
        }
    } catch (err) {
      if (err.response) {
        const response = err.response
        console.error(`API ${url} respond error, response status: ${response.status}, message: ${response.data.error_description}`);
        return { "status": response.status, "message": response.data.error_description, "data": [] };
      } else {
        console.error(`API ${url} connection error.`, err);
        return { "status": -100, "message": err.message, "data": [] };
      }
    }
}

async function sendEmail(
    from = from,  
    to = to,  
    cc=cc,  
    subject = subject,  
    html = html,  
    priority = "normal",  
    attachments=""  
  ) {  
    const transporter = nodemailer.createTransport(config.smtpOptions); 
    let emailObjet = {  
      from:from,  
      to: to,  
      cc:cc,  
      subject: subject,  
      html: html,  
      priority: priority,  
      attachments:attachments  
    };
    
    await transporter.sendMail(emailObjet);
  
  }

function getHostName (referer) {
    return referer.substring(0, referer.indexOf('/', 8))
}
  
exports.sendEmailbySSH = sendEmailbySSH;
exports.compareTime = compareTime;
exports.getCurISOTime = getCurISOTime;
exports.sendSsoRequest = sendSsoRequest;
exports.sendEmail = sendEmail;
exports.getHostName = getHostName;
