"use strict";
process.chdir("fn");

const handler = require("handler");
const express = require("express");
const app = express();

// parse application/json
app.use(express.json());
// parse application/x-www-form-urlencoded with qs package
app.use(express.urlencoded({ extended: true }))
// parse text/*
app.use(express.text({ type: "text/*" }));

app.all("/health", (req, res) => {
  return res.send("OK");
});

app.all("/fn", handler);
app.listen(8000);
