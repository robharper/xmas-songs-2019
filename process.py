import os
import petl
import re
import json

def top10_songs(table):
  return (table.convert('song_title', lambda t: re.sub(r'[\(\[][Ff]eat.*$', '', t))
      .addfield('song_title_id', lambda rec: re.sub(r'\W', '', rec.song_title).lower())
      .aggregate(('song_title_id', 'artist'), {
        'artist_count': len,
        'song_title': lambda r: r[0].song_title,
      })
      .aggregate('song_title_id', {
        'count': ('artist_count', sum),
        'song_title': lambda r: r[0].song_title,
        'artists': (('artist', 'artist_count',), list)
      })
      .sort('count', reverse=True)
      .addrownumbers(start=1, field='rank')
  )

def top10_artists(table):
  return (table.aggregate(('artist'), {
        'count': len,
        'song_title': lambda r: r[0].song_title,
      })
      .sort('count', reverse=True)
      .addrownumbers(start=1, field='rank')
  )

data_2019 = petl.util.base.empty()
for filename in os.listdir('./data/2019/'):
  data_2019 = data_2019.cat(petl.fromjson('./data/2019/'+filename))

data_2018 = petl.util.base.empty()
for filename in os.listdir('./data/2018/'):
  data_2018 = data_2018.cat(petl.fromjson('./data/2018/'+filename))

data_2017 = petl.util.base.empty()
for filename in os.listdir('./data/2017/'):
  data_2017 = data_2017.cat(petl.fromjson('./data/2017/'+filename))

data_2019 = data_2019.distinct('updated_at')
print(data_2019.nrows())

data_2018 = data_2018.distinct('updated_at')
print(data_2018.nrows())

data_2017 = data_2017.distinct('updated_at')
print(data_2017.nrows())

# Fix observed song name changes
name_changes = {
  'Have a Holly Jolly Christmas': 'A Holly Jolly Christmas',
  'Merry Christmas Darling (Remix)': 'Merry Christmas Darling',
  'The Chipmunk Song (feat. Alvin) [Christmas Don\'t Be Late]': 'The Chipmunk Song',
  'Walkin In A Winter Wonderland': 'Winter Wonderland',
  'Santa Claus Is Coming to Town (Intro)': 'Santa Claus Is Coming to Town',
  'Santa Claus Is Comin\' to Town': 'Santa Claus Is Coming to Town',
  'Have Yourself Merry Little Christmas': 'Have Yourself A Merry Little Christmas'
}
data_2017 = data_2017.convert('song_title', name_changes)
data_2018 = data_2018.convert('song_title', name_changes)
data_2019 = data_2019.convert('song_title', name_changes)

# Create normalized song identity columns

print('---------2019--------')
for entry in top10_songs(data_2019).dicts().islice(0,10):
  print(str(entry) + ',')
print('-------')
for entry in top10_artists(data_2019).dicts().islice(0,10):
  print(str(entry) + ',')

print('---------2018--------')
for entry in top10_songs(data_2018).dicts().islice(0,10):
  print(str(entry) + ',')
print('-------')
for entry in top10_artists(data_2018).dicts().islice(0,10):
  print(str(entry) + ',')


print('---------2017--------')
for entry in top10_songs(data_2017).dicts().islice(0,10):
  print(str(entry) + ',')
print('-------')
for entry in top10_artists(data_2017).dicts().islice(0,10):
  print(str(entry) + ',')
