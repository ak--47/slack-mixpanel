import slack from '../services/slack.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import _ from 'highland';

dayjs.extend(utc);
const { md5, clone } = akTools;
const { NODE_ENV = "unknown", company_domain = "mixpanel.com", slack_prefix } = process.env;

async function slackMemberPipeline(startDate, endDate) {
	const slackMemberStream = await slack.analytics(startDate, endDate, "member");
	slackMemberStream.pause();
	
	const slackMembers = await slack.getUsers();
	const slackMemberStreamEv = slackMemberStream.fork();
	const slackMemberStreamProf = slackMemberStream.fork();
	
	const slackMemberEvents = slackMemberStreamEv
		.map((record) => clone(record))
		.filter((user) => user.email_address && user.email_address.endsWith(`@${company_domain}`))
		.map((record) => {
			record.slack_user_id = record.user_id;
			delete record.user_id;
			record.distinct_id = record.email_address.toLowerCase();
			record.event = 'daily user summary';
			record.insert_id = md5(`${record.email_address}-${record.date}-${record.slack_user_id}`);
			record.time = dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix();
			return record;
		})
		.errors(err => {
			if (NODE_ENV === 'dev') console.error('SLACK MEMBERS: error in analytics pipeline', err);
		});

	const slackMemberProfiles = slackMemberStreamProf
		.map((record) => clone(record))
		.filter((user) => user.email_address && user.email_address.endsWith(`@${company_domain}`))
		.map((record) => {
			const memberDetails = slackMembers.find((m) => m.id === record.user_id);
			const profile = {
				distinct_id: record.email_address.toLowerCase(),
				email: record.email_address,
				slack_id: record.user_id,
				slack_team_id: record.team_id,
				'#  → SLACK': `${slack_prefix}/${record.user_id}`,
				is_active: record.is_active				
			};
			
			if (memberDetails && memberDetails.profile) {
				profile.avatar = memberDetails.profile.image_512;
				profile.title = memberDetails.profile.title;
				profile.name = memberDetails.real_name;
				profile.display_name = memberDetails.profile.display_name;
				profile.timezone = memberDetails.tz
			}
			
			return profile;
		})
		.uniqBy((a, b) => {
			return a.distinct_id === b.distinct_id;
		})
		.errors(err => {
			if (NODE_ENV === 'dev') console.error('SLACK MEMBER PROFILES: error in analytics pipeline', err);
		});

	return {
		slackMemberEvents,
		slackMemberProfiles,
	};
}

// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running Slack Members pipeline directly...');
	
	const { NODE_ENV = "unknown" } = process.env;
	
	try {
		// Test with recent dates
		const startDate = dayjs.utc().subtract(4, 'd').format('YYYY-MM-DD');
		const endDate = dayjs.utc().subtract(3, 'd').format('YYYY-MM-DD');
		
		console.log(`📊 Testing member pipeline for ${startDate} to ${endDate}`);
		
		const { slackMemberEvents, slackMemberProfiles } = await slackMemberPipeline(startDate, endDate);
		
		// Convert streams to arrays for inspection
		const [memberEvents, memberProfiles] = await Promise.all([
			new Promise((resolve, _reject) => {
				slackMemberEvents.toArray((results) => {
					resolve(results);
				});
			}),
			
			new Promise((resolve, _reject) => {
				slackMemberProfiles.toArray((results) => {
					resolve(results);
				});
			})
		]);
		
		console.log(`📈 Found ${memberEvents.length} member events`);
		console.log(`👤 Found ${memberProfiles.length} member profiles`);
		
		if (memberEvents.length > 0) {
			console.log('Sample event:', {
				event: memberEvents[0].event,
				distinct_id: memberEvents[0].distinct_id,
				insert_id: memberEvents[0].insert_id
			});
		}
		
		if (memberProfiles.length > 0) {
			console.log('Sample profile:', {
				distinct_id: memberProfiles[0].distinct_id,
				email: memberProfiles[0].email,
				slack_id: memberProfiles[0].slack_id
			});
		}
		
		console.log('✅ Slack Members pipeline test completed successfully!');
		
		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;
			
	} catch (error) {
		console.error('❌ Slack Members pipeline test failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}

export default slackMemberPipeline;