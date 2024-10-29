import { BoxClient, BoxDeveloperTokenAuth } from 'box-typescript-sdk-gen';
import 'dotenv';

const main = async (token: string) => {
    const auth = new BoxDeveloperTokenAuth({ token });
    const client = new BoxClient({ auth });
    const entries = (await client.folders.getFolderItems('0')).entries;
    entries?.forEach((entry) => console.log(entry));
};

main(process.env.BOX_DEVELOPER_TOKEN);
