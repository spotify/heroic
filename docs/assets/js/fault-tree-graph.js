/*global
  d3,data
*/

let duration = 200;
const width = 960,
      height = 800,
      rectW = 124,
      rectH = 90,
      TrectH = 24;
const width_initial = width / 2 - 60;
const DESC_WRAP_LEN = Math.floor(rectW / 5);
const RECT_CENTER = (rectW / 2) - 2;

// Color definitions
const TRANSFER_COLOR = "red",
      STATS_COLOR = "navy";

const GRAPH_SELECTOR = "#fault-tree";

const redraw = () => {
  svg.attr(
    "transform",
    `translate(${d3.event.translate}) scale(${d3.event.scale})`
  );
};

let zm;
const svg = d3
      .select(GRAPH_SELECTOR)
      .append("svg")
      .classed("fault-tree-content", true)
      .attr("width", "100%")
      .attr("height", "100%")
      .call(
        (zm = d3.behavior
         .zoom()
         .scaleExtent([0.05, 5])
         .on("zoom", redraw))
      )
      .append("g")
      .attr("transform", `translate(${width_initial}, 50)`);
zm.translate([width_initial, 20]);
data.x0 = 0;
data.y0 = height / 2;

// Define the div for the tooltip
const tooltip = d3
      .select("body")
      .append("div")
      .style("position", "absolute")
      .style("z-index", "10")
      .style("visibility", "hidden")
      .attr("class", "node-tooltip");

const elbow = d => {
  const sourceY = d.source.y + TrectH,
        sourceX = d.source.x + RECT_CENTER,
        targetY = d.target.y + TrectH + 20,
        targetX = d.target.x + RECT_CENTER;
  const v = (sourceY + targetY) / 2;
  return `M${sourceX},${sourceY}V${v}H${targetX}V${targetY}`;
};

const tree = d3.layout
  .tree()
  .nodeSize([rectW * 1.15, rectH * 1.2])
  .separation((a, b) => (a.parent == b.parent ? 1 : 1.2));

const isTransfer = d => d.type === "transfer";

const update = source => {
  const nodes = tree.nodes(data);
  const links = tree.links(nodes);
  const node = svg.selectAll("g.node").data(nodes, d => d.id);

  const nodeEnter = node
    .enter()
    .append("g")
    .attr("class", "node")
    .attr("transform", d => "translate(" + source.x0 + "," + source.y0 + ")")
    .on("click", toggleCollapse);

  // Create rectangle for node info. Name is in the middle of the box. Long descriptions are displayed on hover.
  nodeEnter
    .append("rect")
    .attr("width", rectW)
    .attr("height", TrectH)
    .attr("stroke", d => (d._children ? "blue" : "black"))
    .attr("stroke-width", 1)
    .style("fill", d => (d._children ? "lightcyan" : "#fff"));
  nodeEnter
    .append("text")
    .attr("x", rectW / 2)
    .attr("y", 14)
    .attr("text-anchor", "middle")
    .text(d => d.name)
    .on('mouseover', d => {
      if (!d.description) return;
      tooltip.text(d.description)
        .style("visibility", "visible")
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
    })
    .on("mouseout", d => tooltip.style("visibility", "hidden"));

  // Shapes for various node types, displayed below the description.
  const orGate = "m 75,65 c  -1.4, -10, .6, -22 -15, -30 -15.6, 8, -13.4, 20, -15, 30, 0, 0 3, -8 15, -8 10, 0 15, 8 15, 8 z";
  const andGate = "m 45,50 0,15 30,0 0,-15  a15,15 .2 0,0 -15,-15 a15,15 .2 0,0 -15,15";
  const priorityGate = "m 45,50 0,15 30,0 0,-15  a15,15 .2 0,0 -15,-15 a15,15 .2 0,0 -15,15 m 0,10 30,0";
  const inhibitGate = "m 60,35 -15,6.340 0,17.3205 15,6.340  15,-6.340 0,-17.3205 z";
  const alarmGate =
    "m 75,65 c  -1.4, -10, .6, -22 -15, -30 -15.6, 8, -13.4, 20, -15, 30, 0, 0 3, -8 15, -8 10, 0 15, 8 15, 8 z m -30,0 v5 c0, 0 3, -8 15, -8 10, 0 15, 8 15, 8 v-5";
  const atLeastGate =  `
m 75,65
c -1.4,-10 .6,-22 -15,-30
  -15.6,8 -13.4,20 -15,30
m 0,0
v 10
h 30
v -10
m -29,-7.5
h 28
`;
  const house = "m 45,50 0,15 30,0 0,-15 -15,-15  -15,15";
  const undeveloped = "m 60,35 m 0,0 l 24,15 l -24,15 l -24,-15 z";
  const component = "m 75,50 a15,15 .2 0,0 -15,-15 a15,15 .2 0,0 -15,15 a15,15 .2 0,0 15,15 a15,15 .2 0,0 15,-15";
  const transferGate = `M${RECT_CENTER},65 H45 L${RECT_CENTER},35 L75,65 Z`;
  nodeEnter
    .append("path")
    .attr("d", d => {
      switch (d.type) {
        case "or":
          return orGate;
        case "and":
          return andGate;
        case "atleast":
          return atLeastGate;
        case "transfer":
          return transferGate;
        default:
          return component;
      }
    })
    .attr({
      stroke: "black",
      "stroke-width": 1,
      "stroke-linejoin": "round",
      fill: "#fff"
    })
    .on('mouseover', d => {
      tooltip.text(`${d.type.toUpperCase()} gate`)
        .style("visibility", "visible")
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
    })
    .on("mouseout", d => tooltip.style("visibility", "hidden"));

  // Display K and N for ATLEAST gates
  nodeEnter
    .append("text")
    .attr("class", "atleast")
    .attr("x", RECT_CENTER)
    .attr("y", TrectH + 45)
    .attr("text-anchor", "middle")
    .attr("fill", "blue")
    .text(d => d.atleast ? `${d.atleast.k} : ${d.atleast.n}` : "");

  // Styling for duplicates
  nodeEnter
    .append("text")
    .attr("x", rectW / 2 - 56)
    .attr("y", TrectH - 26)
    .attr("text-anchor", "middle")
    .attr("fill", TRANSFER_COLOR)
    .text(d => (isTransfer(d) ? "DUP" : ""));

  // Display probability of failure.
  nodeEnter
    .append("text")
    .attr("x", rectW / 2 + 19)
    .attr("y", TrectH + 36)
    .attr("text-anchor", "left")
    .attr("fill", d => isTransfer(d) ? TRANSFER_COLOR : STATS_COLOR)
    .text("Prob");
  nodeEnter
    .append("text")
    .attr("x", rectW / 2 + 19)
    .attr("y", TrectH + 48)
    .attr("text-anchor", "left")
    .attr("fill", d => isTransfer(d) ? TRANSFER_COLOR :  STATS_COLOR)
    .text(d => d.probability.length < 8 ? d.probability : Number(d.probability).toExponential(6));

  const nodeUpdate = node
    .transition()
    .duration(duration)
    .attr("transform", d => `translate(${d.x}, ${d.y})`);

  nodeUpdate
    .select("rect")
    .attr("width", rectW)
    .attr("height", TrectH)
    .attr("stroke", d => (d._children ? "blue" : "black"))
    .attr("stroke-width", 1)
    .style("fill", d => (d._children ? "lightcyan" : "#fff"));
  nodeUpdate.select("text").style("fill-opacity", 1);

  const nodeExit = node
    .exit()
    .transition()
    .duration(duration)
    .attr("transform", d => `translate(${source.x}, ${source.y})`)
    .remove();
  nodeExit
    .select("rect")
    .attr("width", rectW)
    .attr("height", TrectH)
    .attr("stroke", "black")
    .attr("stroke-width", 1);
  nodeExit.select("text");

  // Graph the link between the nodes
  const link = svg.selectAll("path.link").data(links, d => d.target.id);
  link
    .enter()
    .insert("path", "g")
    .attr("class", "node-link")
    .attr("x", rectW / 2)
    .attr("y", rectH / 2)
    .attr("d", d => {
      const o = {
        x: source.x0,
        y: source.y0
      };
      return elbow({
        source: o,
        target: o
      });
    });
  link
    .transition()
    .duration(duration)
    .attr("d", elbow);
  link
    .exit()
    .transition()
    .duration(duration)
    .attr("d", d => {
      const o = {
        x: source.x,
        y: source.y
      };
      return elbow({
        source: o,
        target: o
      });
    })
    .remove();
  nodes.forEach(d => {
    d.x0 = d.x;
    d.y0 = d.y;
  });
};

const toggleCollapse = d => {
  if (d.children) {
    d._children = d.children;
    d.children = null;
  } else {
    d.children = d._children;
    d._children = null;
  }
  update(d);
};

const autocollapse = d => {
  svg.selectAll("g.node").each(d => (d.collapse ? toggleCollapse(d) : null));
};

(() => {
  // Load the initial graph
  const duration_backup = duration;
  duration = 0;
  update(data);
  autocollapse(data);
  duration = duration_backup;
  d3.select(GRAPH_SELECTOR).style("height", "100%");
})();
