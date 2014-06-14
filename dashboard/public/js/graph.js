function drawGraph(data) {
  var margin = 60,
      width = parseInt(d3.select("#graph").style("width")) - margin*2,
      height = parseInt(d3.select("#graph").style("height")) - margin*2;

  var xScale = d3.time.scale()
      .range([0, width])
      .nice(d3.time.year);

  var yScale = d3.scale.linear()
      .range([height, 0])
      .nice();

  var xAxis = d3.svg.axis()
      .scale(xScale)
      .orient("bottom");

  var yAxis = d3.svg.axis()
      .scale(yScale)
      .orient("left");

  var line = d3.svg.line()
      .x(function(d) { return xScale(d.date); })
      .y(function(d) { return yScale(d.close); });

  var graph = d3.select("#graph")
      .attr("width", width + margin*2)
      .attr("height", height + margin*2)
    .append("g")
      .attr("transform", "translate(" + margin + "," + margin + ")");

  data.forEach(function(d) {
    d.date = d3.time.format("%Y-%m-%d").parse(d.date);
    d.close = +d.close;
  });

  xScale.domain(d3.extent(data, function(d) { return d.date; }));
  yScale.domain(d3.extent(data, function(d) { return d.close; }));

  graph.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

  graph.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Price ($)");

  dataPerPixel = data.length/width;
  dataResampled = data.filter(
    function(d, i) { return i % Math.ceil(dataPerPixel) == 0; }
  );

  graph.append("path")
      .datum(dataResampled)
      .attr("class", "line")
      .attr("d", line);

  var firstRecord = data[data.length-1], 
      lastRecord = data[0];

  var first = graph.append("g")
    .attr("class", "first")
    .style("display", "none");

  first.append("text")
    .attr("x", -8)
    .attr("y", 4)
    .attr("text-anchor", "end")
    .text("$" + firstRecord.close);
  first.append("circle")
    .attr("r", 4);


  var last = graph.append("g")
    .attr("class", "last")
    .style("display", "none");

  last.append("text")
    .attr("x", 8)
    .attr("y", 4)
    .text("$" + lastRecord.close);
  last.append("circle")
    .attr("r", 4);

  function resize() {
    var width = parseInt(d3.select("#graph").style("width")) - margin*2,
    height = parseInt(d3.select("#graph").style("height")) - margin*2;

    xScale.range([0, width]).nice(d3.time.year);
    yScale.range([height, 0]).nice();

    if (width < 300 && height < 80) {
      graph.select('.x.axis').style("display", "none");
      graph.select('.y.axis').style("display", "none");
      
      graph.select(".first")
        .attr("transform", "translate(" + xScale(firstRecord.date) + "," + yScale(firstRecord.close) + ")")
        .style("display", "initial");

      graph.select(".last")
        .attr("transform", "translate(" + xScale(lastRecord.date) + "," + yScale(lastRecord.close) + ")")
        .style("display", "initial");
    } else {
      graph.select('.x.axis').style("display", "initial");
      graph.select('.y.axis').style("display", "initial");
      graph.select(".last")
        .style("display", "none");
      graph.select(".first")
        .style("display", "none");
    }

    yAxis.ticks(Math.max(height/50, 2));
    xAxis.ticks(Math.max(width/50, 2));

    graph
      .attr("width", width + margin*2)
      .attr("height", height + margin*2)

    graph.select('.x.axis')
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

    graph.select('.y.axis')
      .call(yAxis);

    dataPerPixel = data.length/width;
    dataResampled = data.filter(
      function(d, i) { return i % Math.ceil(dataPerPixel) == 0; }
    );

    graph.selectAll('.line')
      .datum(dataResampled)
      .attr("d", line);
  }

  d3.select(window).on('resize', resize); 

  resize();
}