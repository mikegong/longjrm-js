import Db from './db.js';

class DbMongo extends Db {
    constructor(conn) {
        super(conn);
    }

    async insert(table, data) {
        const collection = this.conn.db().collection(table);
        let insertResult;
        let insertedDocuments = [];

        if (Array.isArray(data)) {
            insertResult = await collection.insertMany(data);
            if (insertResult.acknowledged) {
                const ids = Object.values(insertResult.insertedIds);
                insertedDocuments = await collection.find({_id: {$in: ids}}).toArray();
            }
        } else {
            insertResult = await collection.insertOne(data);
            if (insertResult.acknowledged) {
                insertedDocuments = [await collection.findOne({_id: insertResult.insertedId})];
            }
        }

        return {
            success: insertResult.acknowledged,
            message: 'Data inserted successfully',
            data: insertedDocuments,
            count: insertedDocuments.length
        };
    }

    async selectAll(table, query = {}) {
        const collection = this.conn.db().collection(table);
        const results = await collection.find(query).toArray();
        return {
            success: true,
            data: results,
            count: results.length
        };
    }

    async update(table, query, data) {
        const collection = this.conn.db().collection(table);
        const result = await collection.updateMany(query, { $set: data });
        if (result.matchedCount > 0) {
            const updatedDocuments = await collection.find(query).toArray();
            return {
                success: true,
                message: 'Data updated successfully',
                data: updatedDocuments,
                count: updatedDocuments.length
            };
        } else {
            return {
                success: false,
                message: 'No documents matched the query. No update performed.',
                data: [],
                count: 0
            };
        }
    }

    async delete(table, query) {
        const collection = this.conn.db().collection(table);
        const documentsToDelete = await collection.find(query).toArray();
        const result = await collection.deleteMany(query);
        if (result.deletedCount > 0) {
            return {
                success: true,
                message: 'Data deleted successfully',
                data: documentsToDelete,
                count: documentsToDelete.length
            };
        } else {
            return {
                success: false,
                message: 'No documents matched the query. No deletion performed.',
                data: [],
                count: 0
            };
        }
    }
}

export default DbMongo;

