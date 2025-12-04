
module.exports = (req, res) => {
  const body = req.body;
  console.log(body);
  const contentType = req.get('Content-Type')
  res.type(contentType)
  res.send(body); 
}
