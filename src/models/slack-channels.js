import slack from '../services/slack.js';
import * as akTools from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import _ from 'highland';

dayjs.extend(utc);
const { md5 } = akTools;
const { NODE_ENV = "unknown", slack_prefix = `https://mixpanel.slack.com/archives` } = process.env;

async function slackChannelPipeline(startDate, endDate) {
	// Get channels lookup table once
	const slackChannels = await slack.getChannels();
	
	// SINGLE API call - convert to array to ensure proper termination
	const slackChannelStream = await slack.analytics(startDate, endDate, "public_channel");
	
	// Convert to array first to avoid hanging fork() streams
	const channelData = await new Promise((resolve, reject) => {
		slackChannelStream.toArray((results) => {
			resolve(results);
		});
	});
	
	// Create independent Highland streams from array data
	const slackChannelEventsStream = _(channelData.slice());
	const slackChannelProfilesStream = _(channelData.slice());

	const slackChannelEvents = slackChannelEventsStream
		.map((record) => {
			// No clone needed - each stream gets independent data
			return {
				...record,
				event: 'daily channel summary',
				insert_id: md5(`${record.channel_id}-${record.date}`),
				time: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix(),
				distinct_id: record.channel_id
			};
		})
		.uniqBy((a, b) => {
			return a.insert_id === b.insert_id;
		})
		.errors((err) => {
			if (NODE_ENV === 'dev') console.error('SLACK CHANNELS: error in analytics pipeline', err);
		});

	const slackChannelProfiles = slackChannelProfilesStream
		.map((record) => {
			const channelDetails = slackChannels.find((c) => c.id === record.channel_id);
			
			const profile = {
				...record,
				distinct_id: record.channel_id,
				date: dayjs.utc(record.date).add(4, 'h').add(20, 'm').unix()
			};
			
			if (channelDetails) {
				profile.name = `#${channelDetails.name}`;
				if (channelDetails?.purpose?.value) profile.purpose = channelDetails.purpose.value;
				if (channelDetails?.topic?.value) profile.topic = channelDetails.topic.value;
				profile['#  → SLACK'] = `${slack_prefix}/${profile.channel_id}`;
				profile["email"] = `${slack_prefix}/${profile.channel_id}`;
				profile.created = dayjs.unix(channelDetails.created).format('YYYY-MM-DD');

				if (channelDetails.is_ext_shared) profile.external = true;
				if (channelDetails.is_shared) profile.external = true;
				if (channelDetails.is_private) profile.private = true;
				if (channelDetails.num_members) profile.members = channelDetails.num_members;
			}
			
			return profile;
		})
		.uniqBy((a, b) => {
			return a.distinct_id === b.distinct_id;
		})
		.errors((err) => {
			if (NODE_ENV === 'dev') console.error('SLACK CHANNEL PROFILES: error in analytics pipeline', err);
		});

	return {
		slackChannelEvents,
		slackChannelProfiles,
	};
}

// Direct execution capability for debugging
if (import.meta.url === `file://${process.argv[1]}`) {
	console.log('🔧 Running Slack Channels pipeline directly...');
	
	const { NODE_ENV = "unknown" } = process.env;
	
	try {
		// Test with recent dates
		const startDate = dayjs.utc().subtract(4, 'd').format('YYYY-MM-DD');
		const endDate = dayjs.utc().subtract(3, 'd').format('YYYY-MM-DD');

		console.log(`📊 Testing channel pipeline for ${startDate} to ${endDate}`);
		
		const { slackChannelEvents, slackChannelProfiles } = await slackChannelPipeline(startDate, endDate);
		
		// Convert streams to arrays for inspection
		const [channelEvents, channelProfiles] = await Promise.all([
			new Promise((resolve, _reject) => {
				slackChannelEvents.toArray((results) => {
					resolve(results);
				});
			}),
			
			new Promise((resolve, _reject) => {
				slackChannelProfiles.toArray((results) => {
					resolve(results);
				});
			})
		]);
		
		console.log(`📈 Found ${channelEvents.length} channel events`);
		console.log(`📺 Found ${channelProfiles.length} channel profiles`);
		
		if (channelEvents.length > 0) {
			console.log('Sample event:', {
				event: channelEvents[0].event,
				distinct_id: channelEvents[0].distinct_id,
				channel_id: channelEvents[0].channel_id,
				insert_id: channelEvents[0].insert_id
			});
		}
		
		if (channelProfiles.length > 0) {
			console.log('Sample profile:', {
				distinct_id: channelProfiles[0].distinct_id,
				name: channelProfiles[0].name,
				private: channelProfiles[0].private,
				external: channelProfiles[0].external
			});
		}
		
		console.log('✅ Slack Channels pipeline test completed successfully!');
		
		// Debugger for dev inspection
		if (NODE_ENV === 'dev') debugger;
			
	} catch (error) {
		console.error('❌ Slack Channels pipeline test failed:', error);
		if (NODE_ENV === 'dev') debugger;
		process.exit(1);
	}
}

export default slackChannelPipeline;