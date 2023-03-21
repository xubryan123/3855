import { useEffect, useState, useRef } from "react";

const Stats = () => {
  const [event, setEvent] = useState([]);

  useEffect(() => {
    fetch("http://localhost:8100/stats")
      .then((res) => res.json())
      .then((res) => {
        setEvent(res);
      });
  }, []);

  return (
    <div className="stats">
      <h2>Latest Statistics</h2>
      <div>
        <p>Max puy price: {event.max_buy_price}</p>
      </div>
    </div>
  );
};

export default Stats;
