import express from "express";
import { DuckDBConnection } from "@duckdb/node-api";
const app = express();

app.use(express.json());

const connection = await DuckDBConnection.create();

await connection.run(`
    CREATE TABLE Teams AS
    SELECT * from read_csv('./teams.csv', types = {'id': 'integer'})
`);

app.get("/teams", async (req, res) => {
	const reader = await connection.runAndReadAll(`SELECT * FROM Teams`);
	const teams = reader.getRowObjects();
	res.json(teams);
});

app.get("/teams/:id", async (req, res) => {
	const id = Number(req.params.id);

	if (isNaN(id)) {
		res.status(404).send("Sorry, we cannot find that team");
	}

	const reader = await connection.runAndReadAll(
		`SELECT * FROM Teams WHERE id = $id`,
		{ id },
	);
	const teams = reader.getRowObjects();
	res.json(teams);
});

app.delete("/teams/:id", async (req, res) => {
	const id = Number(req.params.id);

	if (isNaN(id)) {
		res.status(404).send("Sorry, we cannot find that team");
	}

	const reader = await connection.runAndReadAll(
		`SELECT * FROM Teams WHERE id = $id`,
		{ id },
	);
	const team = reader.getRowObjects();
	await connection.run(`DELETE FROM Teams WHERE id = $id`, { id });
	res.json(team);
});

app.post("/teams/:id", async (req, res) => {
	res.status(404).send("Sorry, we have not implemented this feature yet!");
});

app.listen(3000, () => {
	console.log(`Example app listening on port 3000`);
});
